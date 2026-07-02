package io.celery.redis

import io.celery.deadletter.DeadLetterQueue
import io.celery.deadletter.DeadLetterRecord
import io.celery.deadletter.DeadLetterStats
import io.celery.task.TaskMessage
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.Range
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.toList
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days

/**
 * Redis-based dead letter queue implementation.
 */
@OptIn(ExperimentalLettuceCoroutinesApi::class)
class RedisDeadLetterQueue(
    private val connectionFactory: RedisConnectionFactory,
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val config: RedisDeadLetterConfig = RedisDeadLetterConfig()
) : DeadLetterQueue {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    /**
     * Enqueue a task to dead letter queue.
     */
    override suspend fun enqueue(
        task: TaskMessage,
        reason: String,
        exception: Throwable?
    ) {
        val commands = connectionFactory.getCommands()

        try {
            val record = DeadLetterRecord(
                id = generateId(task),
                task = task,
                reason = reason,
                errorMessage = exception?.message,
                errorType = exception?.javaClass?.name,
                stackTrace = exception?.stackTraceToString(),
                failedAt = Instant.now(),
                originalQueue = task.queue
            )

            val recordJson = json.encodeToString(record)
            val streamId = commands.xadd(
                streamKey(),
                mapOf(
                    "record" to recordJson,
                    "task_name" to task.taskName,
                    "reason" to reason,
                    "failed_at" to record.failedAt.toString()
                )
            )

            // Add to sorted set for time-based queries
            commands.zadd(
                indexKey(),
                record.failedAt.toEpochMilli().toDouble(),
                record.id
            )

            // Store full record
            commands.set(recordKey(record.id), recordJson)
            commands.expire(recordKey(record.id), config.retentionPeriod.inWholeSeconds)

            logger.info("Dead letter queued: ${record.id} (${task.taskName}) - $reason")

        } catch (e: Exception) {
            logger.error("Failed to enqueue dead letter for task: ${task.id}", e)
        }
    }

    /**
     * Replay a dead-lettered task.
     */
    override suspend fun replay(deadLetterId: String): Boolean {
        val record = get(deadLetterId) ?: return false

        val commands = connectionFactory.getCommands()

        try {
            // Update replay count
            val updatedRecord = record.copy(
                replayCount = record.replayCount + 1,
                lastReplayAt = Instant.now()
            )

            commands.set(recordKey(deadLetterId), json.encodeToString(updatedRecord))

            // Add to original queue
            val taskJson = json.encodeToString(record.task)
            val streamKey = streamKey(record.task.queue)
            commands.xadd(streamKey, mapOf("task" to taskJson))

            logger.info("Replayed dead letter: $deadLetterId (attempt ${updatedRecord.replayCount})")
            return true

        } catch (e: Exception) {
            logger.error("Failed to replay dead letter: $deadLetterId", e)
            return false
        }
    }

    /**
     * Replay multiple dead letters.
     */
    override suspend fun replayAll(deadLetterIds: List<String>): Int {
        return deadLetterIds.count { replay(it) }
    }

    /**
     * Replay all dead letters for a task.
     */
    override suspend fun replayByTask(taskName: String, maxCount: Int): Int {
        val records = list(taskName, maxCount)
        return replayAll(records.map { it.id })
    }

    /**
     * Get a dead letter record.
     */
    override suspend fun get(deadLetterId: String): DeadLetterRecord? {
        val commands = connectionFactory.getCommands()

        return try {
            val recordJson = commands.get(recordKey(deadLetterId))
            recordJson?.let { json.decodeFromString<DeadLetterRecord>(it) }
        } catch (e: Exception) {
            logger.error("Failed to get dead letter: $deadLetterId", e)
            null
        }
    }

    /**
     * List dead letter records.
     */
    override suspend fun list(
        taskName: String?,
        limit: Int,
        offset: Int
    ): List<DeadLetterRecord> {
        val commands = connectionFactory.getCommands()

        return try {
            val ids = commands.zrevrange(indexKey(), offset.toLong(), (offset + limit - 1).toLong()).toList()

            if (ids.isEmpty()) return emptyList()

            val keys = ids.map { recordKey(it) }
            val records = commands.mget(*keys.toTypedArray()).toList()

            ids.zip(records.mapNotNull { kv ->
                kv.value?.let {
                    try {
                        json.decodeFromString<DeadLetterRecord>(it)
                    } catch (e: Exception) {
                        null
                    }
                }
            }).filter { (_, record) ->
                taskName == null || record.task.taskName == taskName
            }.map { it.second }

        } catch (e: Exception) {
            logger.error("Failed to list dead letters", e)
            emptyList()
        }
    }

    /**
     * Stream dead letter records.
     */
    override fun stream(taskName: String?): Flow<DeadLetterRecord> = flow {
        val commands = connectionFactory.getCommands()
        var offset = 0L
        val batchSize = 50

        while (currentCoroutineContext().isActive) {
            val ids = commands.zrevrange(indexKey(), offset, offset + batchSize - 1).toList()

            if (ids.isEmpty()) break

            val keys = ids.map { recordKey(it) }
            val records = commands.mget(*keys.toTypedArray()).toList()

            ids.zip(records).forEach { (_, kv) ->
                kv.value?.let {
                    try {
                        val record = json.decodeFromString<DeadLetterRecord>(it)
                        if (taskName == null || record.task.taskName == taskName) {
                            emit(record)
                        }
                    } catch (e: Exception) {
                        // Skip invalid records
                    }
                }
            }

            offset += batchSize
        }
    }.flowOn(Dispatchers.IO)

    /**
     * Delete a dead letter record.
     */
    override suspend fun delete(deadLetterId: String) {
        val commands = connectionFactory.getCommands()

        try {
            commands.del(recordKey(deadLetterId))
            commands.zrem(indexKey(), deadLetterId)
            logger.debug("Deleted dead letter: $deadLetterId")
        } catch (e: Exception) {
            logger.error("Failed to delete dead letter: $deadLetterId", e)
        }
    }

    /**
     * Delete all dead letters for a task.
     */
    override suspend fun deleteByTask(taskName: String): Int {
        val records = list(taskName, Int.MAX_VALUE)
        records.forEach { delete(it.id) }
        return records.size
    }

    /**
     * Purge old dead letters.
     */
    override suspend fun purge(olderThan: Duration): Int {
        val commands = connectionFactory.getCommands()
        var purged = 0

        try {
            val cutoff = Instant.now().minusMillis(olderThan.inWholeMilliseconds)

            // Get old records
            val oldIds = commands.zrangebyscore(
                indexKey(),
                Range.create(
                    0.0,
                    cutoff.toEpochMilli().toDouble()
                )
            )

            // Delete them
            oldIds.collect { id ->
                commands.del(recordKey(id))
                commands.zrem(indexKey(), id)
                purged++
            }

            logger.info("Purged $purged dead letters older than $olderThan")

        } catch (e: Exception) {
            logger.error("Failed to purge dead letters", e)
        }

        return purged
    }

    /**
     * Get dead letter count.
     */
    override suspend fun count(taskName: String?): Long {
        val commands = connectionFactory.getCommands()

        return try {
            if (taskName != null) {
                // Count by scanning (approximate)
                list(taskName, Int.MAX_VALUE).size.toLong()
            } else {
                commands.zcard(indexKey()) ?: 0
            }
        } catch (e: Exception) {
            logger.error("Failed to count dead letters", e)
            0
        }
    }

    /**
     * Get dead letter statistics.
     */
    override suspend fun getStats(): DeadLetterStats {
        val records = list(null, 1000) // Sample up to 1000 records

        return DeadLetterStats(
            totalCount = count(),
            perTask = records.groupBy { it.task.taskName }
                .mapValues { it.value.size.toLong() },
            perReason = records.groupBy { it.reason }
                .mapValues { it.value.size.toLong() },
            perErrorType = records.groupBy { it.errorType ?: "unknown" }
                .mapValues { it.value.size.toLong() },
            oldestEntry = records.minByOrNull { it.failedAt }?.failedAt,
            newestEntry = records.maxByOrNull { it.failedAt }?.failedAt,
            totalReplayed = records.sumOf { it.replayCount.toLong() }
        )
    }

    /**
     * Close dead letter queue.
     */
    override suspend fun close() {
        scope.cancel()
        logger.info("RedisDeadLetterQueue closed")
    }

    // Private helpers

    private fun generateId(task: TaskMessage): String {
        return "dlq-${task.id}-${Instant.now().toEpochMilli()}"
    }

    // Key generation
    private fun streamKey() = "${config.keyPrefix}:dead_letter:stream"
    private fun streamKey(queue: String) = "${config.keyPrefix}:stream:$queue"
    private fun recordKey(deadLetterId: String) = "${config.keyPrefix}:dead_letter:$deadLetterId"
    private fun indexKey() = "${config.keyPrefix}:dead_letter:index"
}

data class RedisDeadLetterConfig(
    val keyPrefix: String = "celery",
    val retentionPeriod: Duration = 30.days,
    val maxRecords: Long = 10000
)