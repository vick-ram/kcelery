package io.celery

import io.lettuce.core.Consumer
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.Limit
import io.lettuce.core.Range
import io.lettuce.core.XGroupCreateArgs
import io.lettuce.core.XReadArgs
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.onEmpty
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class RedisBroker(
    private val redis: RedisCoroutinesCommands<String, String>,
    private val json: Json,
    private val prefix: String = "celery"
) : MessageBroker {
    override suspend fun reject(streamKey: String, group: String, messageId: String, requeue: Boolean) {
        // First acknowledge to remove from pending list
        redis.xack(streamKey, group, messageId)

        if (requeue) {
            // Get the original message and republish
            val messages = redis.xrange(streamKey, Range.create(messageId, messageId))
            messages.collect { streamMessage ->
                val taskJson = streamMessage.body["task"] ?: return@collect
                val task = json.decodeFromString<TaskMessage>(taskJson)
                // Republish to the queue
                publish(task, streamKey.split(":").first())
            }
            // Optionally delete the old message
            redis.xdel(streamKey, messageId)
        } else {
            // Just delete the message if not requeuing
            redis.xdel(streamKey, messageId)
        }
    }

    private val logger = LoggerFactory.getLogger(RedisBroker::class.java)

    override suspend fun publish(task: TaskMessage, queue: String) {
        val taskJson = json.encodeToString(task)

        if (task.eta != null && task.eta > System.currentTimeMillis()) {
            // Schedule for later
            scheduleTask(task)
        } else {
            // Ready to execute now
            val score = if (task.priority > 0) -task.priority.toDouble() else 0.0
            redis.zadd(scheduledKey(), score, task.id)
            redis.set(taskKey(task.id), taskJson)
            redis.expire(taskKey(task.id), 86400) // 24h TTL
        }
    }

    override suspend fun scheduleTask(task: TaskMessage) {
        val score = task.eta?.toDouble() ?: System.currentTimeMillis().toDouble()
        redis.zadd(scheduledKey(), score, task.id)
        redis.set(taskKey(task.id), json.encodeToString(task))
    }

    override fun consume(
        queue: String,
        consumerGroup: String,
        consumerName: String
    ): Flow<BrokerRecord> = channelFlow {

        val streamKey = streamKey(queue)

        // Create consumer group if needed
        try {
            redis.xgroupCreate(
                XReadArgs.StreamOffset.from(streamKey, "0"),
                consumerGroup,
                XGroupCreateArgs.Builder.mkstream(true)
            )
        } catch (_: Exception) {
            // BUSYGROUP = already exists
        }

        // Scheduled task recovery worker
        launch {
            while (isActive) {
                try {
                    recoverScheduledTasks(queue)
                } catch (e: Exception) {
                    logger.error(
                        "Failed scheduled recovery for queue=$queue",
                        e
                    )
                }

                delay(30.seconds)
            }
        }

        // Pending message recovery worker
        launch {
            while (isActive) {
                try {
                    recoverPending(
                        queue,
                        consumerGroup,
                        consumerName,
                        ::send
                    )
                } catch (e: Exception) {
                    logger.error(
                        "Failed pending recovery for queue=$queue",
                        e
                    )
                }

                delay(10.seconds)
            }
        }

        // Main consumer loop
        while (isActive) {
            val messages = redis.xreadgroup(
                Consumer.from(
                    consumerGroup,
                    consumerName
                ),
                XReadArgs.Builder
                    .count(10)
                    .block(5000),
                XReadArgs.StreamOffset.lastConsumed(streamKey)
            )

            messages
                .catch {
                    logger.error(
                        "Consumer error queue=$queue",
                        it
                    )

                    delay(1.seconds)
                }
                .onEmpty { return@onEmpty }
                .collect { message ->

                    try {
                        val taskJson =
                            message.body["task"] ?: return@collect

                        val task =
                            json.decodeFromString<TaskMessage>(
                                taskJson
                            )

                        // Expiration check
                        if (
                            task.expires != null &&
                            System.currentTimeMillis() > task.expires
                        ) {

                            redis.xack(
                                streamKey,
                                consumerGroup,
                                message.id
                            )

                            handleExpiredTask(task)
                        }

                        send(
                            BrokerRecord(
                                messageId = message.id,
                                payload = task,
                                streamKey = streamKey
                            )
                        )

                    } catch (e: Exception) {

                        logger.error(
                            "Failed processing stream message ${message.id}",
                            e
                        )

                        // Prevent poison messages
                        redis.xack(
                            streamKey,
                            consumerGroup,
                            message.id
                        )
                    }
                }
        }
    }

    private suspend fun recoverScheduledTasks(queue: String) {
        while (currentCoroutineContext().isActive) {
            val now = System.currentTimeMillis()
            // Get tasks whose ETA has passed
            val due = redis.zrangebyscore(scheduledKey(), Range.create(0.0, now.toDouble()), Limit.from( 10))


            due
                .collect { taskId ->
                val taskJson = redis.get(taskKey(taskId)) ?: return@collect
                val task = json.decodeFromString<TaskMessage>(taskJson)
                val streamKey = streamKey(task.queue)

                    // Move to ready queue
                    redis.xadd(streamKey, mapOf("task" to taskJson))
                    redis.zrem(scheduledKey(), taskId)
                    redis.del(taskKey(taskId))
                }
        }
    }

    private suspend fun recoverPending(
        queue: String,
        consumerGroup: String,
        consumerName: String,
        onRecord: suspend (BrokerRecord) -> Unit
    ) {
        val streamKey = streamKey(queue)

        // Get pending messages older than 30 seconds
        val pendingFlow = redis.xpending(
            streamKey,
            consumerGroup,
            range = Range.unbounded(), // "-" to "+"
            limit = Limit.from(10)
        )

        pendingFlow.collect { pendingMessage ->
            val idleTime = Duration.ofMillis(pendingMessage.msSinceLastDelivery)
            if (idleTime > Duration.ofSeconds(30)) {
                // Claim and redeliver
                val claimed = redis.xclaim(
                    streamKey,
                    Consumer.from(consumerGroup, consumerName),
                    minIdleTime = 30000,
                    messageIds = arrayOf(pendingMessage.id)
                )

                claimed.collect { streamMessage ->
                    val taskJson = streamMessage.body["task"] ?: return@collect
                    val task = json.decodeFromString<TaskMessage>(taskJson)
                    onRecord(BrokerRecord(streamMessage.id, task, streamKey))
                }
            }
        }
    }

    override suspend fun acknowledge(streamKey: String, group: String, messageId: String) {
        redis.xack(streamKey, group, messageId)
        redis.xdel(streamKey, messageId)
    }

    override suspend fun claimPending(
        queue: String,
        group: String,
        consumerName: String,
        minIdleTime: Duration
    ) {
        val streamKey = streamKey(queue)

        val pendingFlow = redis.xpending(
            streamKey,
            group,
            range = Range.unbounded(),
            limit = Limit.from(100)
        )

        pendingFlow.collect { pendingMessage ->
            if (pendingMessage.msSinceLastDelivery > minIdleTime.toMillis()) {
                redis.xclaim(
                    streamKey,
                    Consumer.from(group, consumerName),
                    minIdleTime = minIdleTime.toMillis(),
                    messageIds = arrayOf(pendingMessage.id)
                ).collect {
                    // Collect the flow to execute the claim
                    // If you don't need the results, you can ignore them
                }
            }
        }
    }

    override suspend fun close() {
        // Redis connection managed externally
    }

    private fun streamKey(queue: String) = "$prefix:stream:$queue"
    private fun taskKey(taskId: String) = "$prefix:task:$taskId"
    private fun scheduledKey() = "$prefix:scheduled"
    private fun deadLetterKey() = "$prefix:dead"

    private suspend fun handleExpiredTask(task: TaskMessage) {
        logger.info("Task ${task.id} expired")
        // Store in dead letter queue for analysis
        redis.xadd(
            deadLetterKey(), mapOf(
                "task" to json.encodeToString(task),
                "reason" to "expired"
            )
        )
    }
}