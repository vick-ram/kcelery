package io.celery.redis

import io.celery.deadletter.DeadLetterQueue
import io.celery.deadletter.DeadLetterRecord
import io.celery.deadletter.DeadLetterStats
import io.celery.task.TaskMessage
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.Limit
import io.lettuce.core.Range
import io.lettuce.core.api.coroutines.BaseRedisCoroutinesCommands
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.flow.toList
import kotlinx.serialization.decodeFromString
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

    /**
     * Enqueue a task to dead letter queue.
     */
    override suspend fun enqueue(
        task: TaskMessage,
        reason: String,
        exception: Throwable?
    ) {
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
        try {
            connectionFactory.withCommands { commands ->
                commands.xadd(
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

            }
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

        return try {
            connectionFactory.withCommands { commands ->
                val updated = record.copy(
                    replayCount = record.replayCount + 1,
                    lastReplayAt = Instant.now()
                )
                commands.set(recordKey(deadLetterId), json.encodeToString<DeadLetterRecord>(updated))
                commands.xadd(
                    streamKey(record.task.queue),
                    mapOf("task" to json.encodeToString<TaskMessage>(record.task))
                )
            }
            logger.info("Replayed dead letter $deadLetterId (attempt ${record.replayCount + 1})")
            true
        } catch (e: Exception) {
            logger.error("Failed to replay dead letter $deadLetterId", e)
            false
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
    override suspend fun get(deadLetterId: String): DeadLetterRecord? = try {
        connectionFactory.withCommands { commands ->
            commands.get(recordKey(deadLetterId))
                ?.let { json.decodeFromString<DeadLetterRecord>(it) }
        }
    } catch (e: Exception) {
        logger.error("Failed to get dead letter: $deadLetterId", e)
        null
    }

    /**
     * List dead letter records, newest first.
     * When [taskName] is non-null, filters by task name.
     * Filtering is done in Redis via the index scan — records not matching
     * the task name are skipped without loading their full payloads first,
     * so this doesn't pull all records into memory.
     */
    override suspend fun list(
        taskName: String?,
        limit: Int,
        offset: Int
    ): List<DeadLetterRecord> = try {
        connectionFactory.withCommands { commands ->
            if (taskName != null) {
                // Scan the index until we have [limit] matching records.
                // We page through in batches to avoid loading the full DLQ.
                val result = mutableListOf<DeadLetterRecord>()
                var cursor = offset.toLong()
                val batchSize = (limit * 2).coerceAtLeast(50).toLong()

                while (result.size < limit) {
                    val ids = commands.zrevrange(indexKey(), cursor, cursor + batchSize - 1).toList()
                    if (ids.isEmpty()) break

                    val keys = ids.map { recordKey(it) }
                    commands.mget(*keys.toTypedArray()).toList()
                        .mapNotNull { kv ->
                            kv.value?.let {
                                runCatching { json.decodeFromString<DeadLetterRecord>( it) }.getOrNull()
                            }
                        }
                        .filter { it.task.taskName == taskName }
                        .forEach { if (result.size < limit) result.add(it) }

                    cursor += batchSize
                    if (ids.size < batchSize) break
                }
                result
            } else {
                val ids = commands.zrevrange(indexKey(), offset.toLong(), (offset + limit - 1).toLong()).toList()
                if (ids.isEmpty()) return@withCommands emptyList()

                val keys = ids.map { recordKey(it) }
                commands.mget(*keys.toTypedArray()).toList()
                    .mapNotNull { kv ->
                        kv.value?.let {
                            runCatching { json.decodeFromString<DeadLetterRecord>(it) }.getOrNull()
                        }
                    }
            }
        }
    } catch (e: Exception) {
        logger.error("Failed to list dead letters", e)
        emptyList()
    }

    /** Streams all records without loading them all into memory at once. */
    override fun stream(taskName: String?): Flow<DeadLetterRecord> = flow {
        var offset = 0L
        val batchSize = 50L

        while (currentCoroutineContext().isActive) {
            val batch = connectionFactory.withCommands { commands ->
                val ids = commands.zrevrange(indexKey(), offset, offset + batchSize - 1).toList()
                if (ids.isEmpty()) return@withCommands emptyList()

                val keys = ids.map { recordKey(it) }
                commands.mget(*keys.toTypedArray()).toList()
                    .mapNotNull { kv -> kv.value?.let {
                        runCatching { json.decodeFromString<DeadLetterRecord>( it) }.getOrNull()
                    }}
                    .filter { taskName == null || it.task.taskName == taskName }
            }

            if (batch.isEmpty()) break
            batch.forEach { emit(it) }
            offset += batchSize
        }
    }

    /**
     * Delete a dead letter record.
     */
    override suspend fun delete(deadLetterId: String) {
        try {
            connectionFactory.withCommands { commands ->
                commands.del(recordKey(deadLetterId))
                commands.zrem(indexKey(), deadLetterId)
            }
            logger.debug("Deleted dead letter $deadLetterId")
        } catch (e: Exception) {
            logger.error("Failed to delete dead letter $deadLetterId", e)
        }
    }

    /**
     * Delete all dead letters for a task.
     */
    override suspend fun deleteByTask(taskName: String, batchSize: Int): Int {
        var deleted = 0
        var offset = 0
        while (true) {
            val ids = idsForTask(taskName, batchSize, offset)
            if (ids.isEmpty()) break
            ids.forEach { delete(it) }
            deleted += ids.size
            if (ids.size < batchSize) break
            offset += batchSize
        }
        return deleted
    }

    /**
     * Purge records older than [olderThan]. Batches the deletes to avoid
     * holding a large list in memory.
     */
    override suspend fun purge(olderThan: Duration): Int {
        var purged = 0
        val cutoff = Instant.now().minusMillis(olderThan.inWholeMilliseconds).toEpochMilli().toDouble()

        try {
            while (true) {
                val batch = connectionFactory.withCommands { commands ->
                    commands.zrangebyscore(
                        indexKey(),
                        Range.create(0.0, cutoff),
                        Limit.from(100)
                    ).toList()
                }
                if (batch.isEmpty()) break

                connectionFactory.withCommands { commands ->
                    // del accepts vararg — one roundtrip for the whole batch
                    commands.del(*batch.map { recordKey(it) }.toTypedArray())
                    batch.forEach { commands.zrem(indexKey(), it) }
                }
                purged += batch.size
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
    override suspend fun count(taskName: String?): Long = try {
        connectionFactory.withCommands { commands ->
            if (taskName != null) {
                // Scan the index; cheaper than loading full records
                idsForTask(taskName, Int.MAX_VALUE).size.toLong()
            } else {
                commands.zcard(indexKey()) ?: 0L
            }
        }
        } catch (e: Exception) {
            logger.error("Failed to count dead letters", e)
            0
    }

/**
 * Stats derived from the same page of records so the counts are
 * internally consistent. [sampleSize] controls the window — larger
 * values give more accurate breakdowns but cost more Redis reads.
 */
override suspend fun getStats(sampleSize: Int): DeadLetterStats {
    val sample = list(limit = sampleSize)
    val total = count()
    return DeadLetterStats(
        totalCount    = total,
        sampleSize    = sample.size,
        perTask       = sample.groupBy { it.task.taskName }.mapValues { it.value.size.toLong() },
        perReason     = sample.groupBy { it.reason }.mapValues { it.value.size.toLong() },
        perErrorType  = sample.groupBy { it.errorType ?: "unknown" }.mapValues { it.value.size.toLong() },
        oldestEntry   = sample.minByOrNull { it.failedAt }?.failedAt,
        newestEntry   = sample.maxByOrNull { it.failedAt }?.failedAt,
        totalReplayed = sample.sumOf { it.replayCount.toLong() }
    )
}

    /** Returns IDs for records matching [taskName], without loading full payloads. */
    private suspend fun idsForTask(taskName: String, limit: Int, offset: Int = 0): List<String> {
        if (limit <= 0) return emptyList()
        val result = mutableListOf<String>()
        var cursor = offset.toLong()
        val batchSize = 100L

        while (result.size < limit) {
            connectionFactory.withCommands { commands ->
                val ids = commands.zrevrange(indexKey(), cursor, cursor + batchSize - 1).toList()
                if (ids.isEmpty()) return@withCommands result

                val keys = ids.map { recordKey(it) }
                commands.mget(*keys.toTypedArray()).toList()
                    .zip(ids) { kv, id ->
                        kv.value
                            ?.let { runCatching { json.decodeFromString<DeadLetterRecord>( it) }.getOrNull() }
                            ?.takeIf { it.task.taskName == taskName }
                            ?.let { result.add(id) }
                    }
                cursor += batchSize
                if (ids.size < batchSize) return@withCommands result
            }
        }
        return result.take(limit)
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