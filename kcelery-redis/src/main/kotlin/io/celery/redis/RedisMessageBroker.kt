package io.celery.redis

import io.celery.broker.BrokerPublishException
import io.celery.broker.BrokerRecord
import io.celery.broker.MessageBroker
import io.celery.task.TaskMessage
import io.lettuce.core.*
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.milliseconds

/**
 * Redis Streams-based message broker implementation.
 * Uses Redis Streams with consumer groups for reliable message delivery.
 */
@OptIn(ExperimentalLettuceCoroutinesApi::class)
class RedisMessageBroker(
    private val connectionFactory: RedisConnectionFactory,
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val config: RedisBrokerConfig = RedisBrokerConfig()
) : MessageBroker {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    // Active consumers for cleanup
    private val activeConsumers = ConcurrentHashMap<String, Job>()

    /**
     * Enqueue a task message to a queue.
     */
    override suspend fun enqueue(task: TaskMessage, queue: String) {
        val streamKey = streamKey(queue)

        try {
            val taskJson = json.encodeToString(task)
            connectionFactory.withCommands { commands ->

                // Add to stream
                val messageId = commands.xadd(
                    streamKey,
                    mapOf(
                        "task" to taskJson,
                        "task_name" to task.taskName,
                        "priority" to task.priority.toString(),
                        "created_at" to task.createdAt.toString()
                    )
                )

                // If message has ETA, add to sorted set for scheduling
                val eta = task.eta
                if (eta != null && eta > System.currentTimeMillis()) {
                    commands.zadd(
                        scheduledKey(),
                        eta.toDouble(),
                        task.id
                    )
                    // Store task data for later retrieval
                    commands.set(taskKey(task.id), taskJson)
                    commands.expire(taskKey(task.id), 86400) // 24h TTL
                }
                logger.debug("Enqueued task ${task.id} to $queue (message: $messageId)")
            }

        } catch (e: Exception) {
            logger.error("Failed to enqueue task ${task.id} to $queue", e)
            throw BrokerPublishException("Failed to enqueue task", e)
        }
    }

    /**
     * Consume messages from a queue as a Flow.
     */
//    override fun consume(
//        queue: String,
//        consumerGroup: String,
//        consumerName: String,
//        batchSize: Int,
//        pollTimeout: Duration
//    ): Flow<BrokerRecord> = flow {
//        val streamKey = streamKey(queue)
//        val consumerKey = "$consumerGroup-$consumerName"
//
//        try {
//            connectionFactory.withCommands { commands ->
//                // Create consumer group if not exists
//                ensureConsumerGroup(commands, streamKey, consumerGroup)
//
//                // Start scheduler recovery
//                val schedulerJob = scope.launch {
//                    recoverScheduledTasks( queue)
//                }
//
//                // Track this consumer
//                activeConsumers[consumerKey] = currentCoroutineContext()[Job]!!
//
//                while (currentCoroutineContext().isActive) {
//                    try {
//                        val consumer = Consumer.from(consumerGroup, consumerName)
////                        val streamOffset = XReadArgs.StreamOffset.latest(streamKey)
//                        val streamOffset = XReadArgs.StreamOffset.from(streamKey, ">")
//                        val readArgs = XReadArgs.Builder.count(batchSize.toLong())
//                            .block(Duration.ofMillis(pollTimeout.toMillis()))
//
//                        commands.xreadgroup(consumer, readArgs, streamOffset).collect { message ->
//                            if (message.stream != streamKey) return@collect
//
//                            val taskJson = message.body["task"] ?: return@collect
//                            try {
//                                val task = json.decodeFromString<TaskMessage>(taskJson)
//
//                                // Check expiration
//                                if (task.isExpired()) {
//                                    commands.xack(streamKey, consumerGroup, message.id)
//                                    commands.xdel(streamKey, message.id)
//                                    logger.info("Task ${task.id} expired, removed from queue")
//                                    return@collect
//                                }
//                                val record = BrokerRecord(
//                                    messageId = message.id,
//                                    payload = task,
//                                    streamKey = streamKey,
//                                    group = consumerGroup,
//                                    consumer = consumerName
//                                )
//                                emit(record)
//                            } catch (e: Exception) {
//                                logger.error("Failed to deserialize message ${message.id}", e)
//                                // Acknowledge bad messages to prevent blocking
//                                commands.xack(streamKey, consumerGroup, message.id)
//                            }
//                        }
//
//                        // Recover pending messages
//                        recoverPending(commands, streamKey, consumerGroup, consumerName)
//
//                    } catch (e: TimeoutCancellationException) {
//                        // Normal timeout, continue polling
//                    } catch (e: CancellationException) {
//                        throw e
//                    } catch (e: Exception) {
//                        logger.error("Error consuming from $queue", e)
//                        delay(1.milliseconds)
//                    }
//                }
//            }
//
//        } finally {
//            activeConsumers.remove(consumerKey)
//            logger.info("Consumer $consumerKey stopped")
//        }
//    }.flowOn(Dispatchers.IO)
    override fun consume(
        queue: String,
        consumerGroup: String,
        consumerName: String,
        batchSize: Int,
        pollTimeout: Duration
    ): Flow<BrokerRecord> = flow {
        val streamKey = streamKey(queue)
        val consumerKey = "$consumerGroup-$consumerName"

        // 1. Setup metadata on an ephemeral checkout
        connectionFactory.withCommands { commands ->
            ensureConsumerGroup(commands, streamKey, consumerGroup)
        }

        val schedulerJob = scope.launch {
            recoverScheduledTasks(queue)
        }

        activeConsumers[consumerKey] = currentCoroutineContext()[Job]!!

        val consumer = Consumer.from(consumerGroup, consumerName)
        val readArgs = XReadArgs.Builder.count(batchSize.toLong())
            .block(java.time.Duration.ofMillis(pollTimeout.toMillis()))
        val streamOffset = XReadArgs.StreamOffset.from(streamKey, ">")

        while (currentCoroutineContext().isActive) {
            try {
                // 2. Fetch data, then immediately drop the connection back to the pool
                val messages = connectionFactory.withCommands { commands ->
                    // Note: Change from reactive .collect to a direct coroutine call
                    // If using Lettuce Coroutines, call the suspending variation that returns a List
                    commands.xreadgroup(consumer, readArgs, streamOffset)
                }

                // 3. Process records outside of the connection-borrow context
                messages.collect { message ->
                    if (message.stream != streamKey) return@collect
                    val taskJson = message.body["task"] ?: return@collect

                    try {
                        val task = json.decodeFromString<TaskMessage>(taskJson)
                        if (task.isExpired()) {
                            connectionFactory.withCommands { c ->
                                c.xack(streamKey, consumerGroup, message.id)
                                c.xdel(streamKey, message.id)
                            }
                            return@collect
                        }

                        emit(
                            BrokerRecord(
                                messageId = message.id,
                                payload = task,
                                streamKey = streamKey,
                                group = consumerGroup,
                                consumer = consumerName
                            )
                        )
                    } catch (e: Exception) {
                        connectionFactory.withCommands { c -> c.xack(streamKey, consumerGroup, message.id) }
                    }
                }

                // Periodic clean-ups can run on short checkouts too
                connectionFactory.withCommands { commands ->
                    recoverPending(commands, streamKey, consumerGroup, consumerName)
                }

            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logger.error("Error consuming from $queue", e)
                delay(100.milliseconds)
            }
        }
    }.flowOn(Dispatchers.IO)

    /**
     * Acknowledge successful processing.
     */
    override suspend fun ack(record: BrokerRecord) {
        try {
            connectionFactory.withCommands { commands ->
                commands.xack(record.streamKey, record.group, record.messageId)
                commands.xdel(record.streamKey, record.messageId)
                logger.debug("Acknowledged message: ${record.messageId}")
            }
        } catch (e: Exception) {
            logger.error("Failed to ack message: ${record.messageId}", e)
        }
    }

    /**
     * Negative-acknowledge (reject) a message.
     */
    override suspend fun nack(record: BrokerRecord, requeue: Boolean) {
        try {
            if (requeue) {
                // Let it stay pending for retry
                logger.debug("NACK with requeue for message: ${record.messageId}")
            } else {
                connectionFactory.withCommands { commands ->
                    // Acknowledge and delete
                    commands.xack(record.streamKey, record.group, record.messageId)
                    commands.xdel(record.streamKey, record.messageId)
                }
                logger.debug("NACK without requeue for message: ${record.messageId}")
            }
        } catch (e: Exception) {
            logger.error("Failed to nack message: ${record.messageId}", e)
        }
    }

    /**
     * Move a message to dead letter queue.
     */
    override suspend fun deadLetter(record: BrokerRecord, reason: String) {
        try {
            val dlqKey = deadLetterKey()
            val taskJson = json.encodeToString(record.payload)

            connectionFactory.withCommands { commands ->
                commands.xadd(
                    dlqKey,
                    mapOf(
                        "task" to taskJson,
                        "reason" to reason,
                        "original_queue" to record.streamKey,
                        "failed_at" to java.time.Instant.now().toString()
                    )
                )

                // Remove from original queue
                commands.xack(record.streamKey, record.group, record.messageId)
                commands.xdel(record.streamKey, record.messageId)
            }

            logger.info("Moved message ${record.messageId} to DLQ: $reason")

        } catch (e: Exception) {
            logger.error("Failed to move message to DLQ: ${record.messageId}", e)
        }
    }

    /**
     * Schedule a task for future execution.
     */
    override suspend fun scheduleTask(task: TaskMessage) {
        val score = task.eta?.toDouble() ?: System.currentTimeMillis().toDouble()

        try {
            connectionFactory.withCommands { commands ->
                commands.zadd(scheduledKey(), score, task.id)
                commands.set(taskKey(task.id), json.encodeToString(task))
            }
            logger.debug("Scheduled task ${task.id} for $score")
        } catch (e: Exception) {
            logger.error("Failed to schedule task ${task.id}", e)
            throw BrokerPublishException("Failed to schedule task", e)
        }
    }

    /**
     * Get queue depth.
     */
    override suspend fun queueSize(queue: String): Long {
        return try {
            connectionFactory.withCommands { commands ->
                commands.xlen(streamKey(queue))
            }
        } catch (e: Exception) {
            logger.error("Failed to get queue size for $queue", e)
            0
        }!!
    }

    /**
     * Purge all messages from a queue.
     */
    override suspend fun purgeQueue(queue: String) {
        val streamKey = streamKey(queue)
        try {
            connectionFactory.withCommands { commands ->
                commands.del(streamKey)
            }
            logger.info("Purged queue: $queue")
        } catch (e: Exception) {
            logger.error("Failed to purge queue: $queue", e)
        }
    }

    /**
     * Close broker connections.
     */
    override suspend fun close() {
        // Cancel all active consumers
        activeConsumers.values.forEach { it.cancel() }
        activeConsumers.clear()

        scope.cancel()
        connectionFactory.close()
        logger.info("RedisMessageBroker closed")
    }

    /**
     * Health check.
     */
    override suspend fun healthCheck(): Boolean {
        return connectionFactory.healthCheck()
    }

    // Private helper methods

    private suspend fun ensureConsumerGroup(
        commands: RedisCoroutinesCommands<String, String>,
        streamKey: String,
        consumerGroup: String
    ) {
        try {
            // Try to create consumer group
            commands.xgroupCreate(
                XReadArgs.StreamOffset.from(streamKey, "0"),
                consumerGroup,
                XGroupCreateArgs.Builder.mkstream(true)
            )
            logger.info("Created consumer group: $consumerGroup for stream: $streamKey")
        } catch (e: Exception) {
            if (e.message?.contains("BUSYGROUP") == true) {
                // Group already exists, that's fine
                logger.debug("Consumer group $consumerGroup already exists")
            } else {
                logger.error("Failed to create consumer group: $consumerGroup", e)
            }
        }
    }

    private suspend fun recoverScheduledTasks(
        queue: String
    ) {
        while (currentCoroutineContext().isActive) {
            try {
                val now = System.currentTimeMillis().toDouble()
                connectionFactory.withCommands { commands ->

                // Get tasks whose ETA has passed
                val dueTaskIds = commands.zrangebyscore(
                    scheduledKey(),
                    Range.create(0.0, now),
                    Limit.from(10)
                )

                dueTaskIds.collect { taskId ->
                    val taskJson = commands.get(taskKey(taskId))
                    if (taskJson != null) {
                        try {
                            val task = json.decodeFromString<TaskMessage>(taskJson)
                            val streamKey = streamKey(task.queue)

                            // Add to stream
                            commands.xadd(streamKey, mapOf("task" to taskJson))

                            // Remove from scheduled set
                            commands.zrem(scheduledKey(), taskId)
                            commands.del(taskKey(taskId))

                            logger.debug("Recovered scheduled task: ${task.id}")
                        } catch (e: Exception) {
                            logger.error("Failed to recover scheduled task: $taskId", e)
                            // Remove invalid scheduled task
                            commands.zrem(scheduledKey(), taskId)
                            commands.del(taskKey(taskId))
                        }
                    } else {
                        // Task data missing, clean up
                        commands.zrem(scheduledKey(), taskId)
                    }
                }

                if (dueTaskIds.toList().isEmpty()) {
                    delay(1.milliseconds)
                }
            }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                logger.error("Error recovering scheduled tasks", e)
                delay(5.milliseconds)
            }
        }
    }

    private suspend fun recoverPending(
        commands: RedisCoroutinesCommands<String, String>,
        streamKey: String,
        consumerGroup: String,
        consumerName: String
    ) {
        try {
            // Get pending messages older than 30 seconds
            val pendingResult = commands.xpending(
                streamKey,
                consumerGroup,
                Range.create("-", "+"),
                Limit.from(10)
            )

            pendingResult.collect { pending ->
                if (pending.msSinceLastDelivery > 30000) { // 30 seconds idle
                    try {
                        // Claim and redeliver
                        commands.xclaim(
                            streamKey,
                            Consumer.from(consumerGroup, consumerName),
                            30_000L,
                            pending.id
                        ).collect()
                        logger.debug("Claimed pending message: ${pending.id} (idle: ${pending.msSinceLastDelivery}ms)")
                    } catch (e: Exception) {
                        logger.error("Failed to claim pending message: ${pending.id}", e)
                    }
                }
            }
        } catch (e: Exception) {
            // XPENDING may throw if no pending messages
            logger.trace("No pending messages to recover")
        }
    }

    // Key generation helpers
    private fun streamKey(queue: String) = "${config.keyPrefix}:stream:$queue"
    private fun taskKey(taskId: String) = "${config.keyPrefix}:task:$taskId"
    private fun scheduledKey() = "${config.keyPrefix}:scheduled"
    private fun deadLetterKey() = "${config.keyPrefix}:dead"
}

data class RedisBrokerConfig(
    val keyPrefix: String = "celery",
    val messageTtl: Long = 86400, // 24 hours
    val maxStreamLength: Long = 10000,
    val pendingMessageTimeout: Long = 30000 // 30 seconds
)