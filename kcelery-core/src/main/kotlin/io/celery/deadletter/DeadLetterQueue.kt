package io.celery.deadletter

import io.celery.task.TaskMessage
import kotlinx.coroutines.flow.Flow
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days

/**
 * Dead letter queue for handling failed tasks.
 * Provides analysis, replay, and cleanup capabilities.
 */
interface DeadLetterQueue {
    /**
     * Move a task to the dead letter queue.
     * 
     * @param task The failed task message
     * @param reason Reason for failure
     * @param exception The exception that caused the failure
     */
    suspend fun enqueue(task: TaskMessage, reason: String, exception: Throwable? = null)

    /**
     * Replay a dead-lettered task (send back to original queue).
     * 
     * @param deadLetterId The dead letter record ID
     * @return true if replay was successful
     */
    suspend fun replay(deadLetterId: String): Boolean

    /**
     * Replay multiple dead-lettered tasks.
     * 
     * @param deadLetterIds List of dead letter record IDs
     * @return Number of successfully replayed tasks
     */
    suspend fun replayAll(deadLetterIds: List<String>): Int

    /**
     * Replay all dead letters for a specific task.
     * 
     * @param taskName Task name to replay
     * @param maxCount Maximum number to replay
     * @return Number of successfully replayed tasks
     */
    suspend fun replayByTask(taskName: String, maxCount: Int = 100): Int

    /**
     * Get a specific dead letter record.
     * 
     * @param deadLetterId The dead letter record ID
     * @return The dead letter record, or null if not found
     */
    suspend fun get(deadLetterId: String): DeadLetterRecord?

    /**
     * List dead letter records.
     * 
     * @param taskName Optional task name filter
     * @param limit Maximum number to return
     * @param offset Offset for pagination
     * @return List of dead letter records
     */
    suspend fun list(
        taskName: String? = null,
        limit: Int = 50,
        offset: Int = 0
    ): List<DeadLetterRecord>

    /**
     * Stream dead letter records.
     * 
     * @param taskName Optional task name filter
     * @return Flow of dead letter records
     */
    fun stream(taskName: String? = null): Flow<DeadLetterRecord>

    /**
     * Delete a specific dead letter record.
     * 
     * @param deadLetterId The dead letter record ID
     */
    suspend fun delete(deadLetterId: String)

    /**
     * Delete all dead letters for a specific task.
     * 
     * @param taskName Task name
     * @return Number of deleted records
     */
    suspend fun deleteByTask(taskName: String): Int

    /**
     * Purge all dead letter records older than a duration.
     * 
     * @param olderThan Duration threshold
     * @return Number of purged records
     */
    suspend fun purge(olderThan: Duration = 30.days): Int

    /**
     * Get count of dead letter records.
     * 
     * @param taskName Optional task name filter
     * @return Number of dead letter records
     */
    suspend fun count(taskName: String? = null): Long

    /**
     * Get dead letter statistics.
     * 
     * @return Dead letter statistics
     */
    suspend fun getStats(): DeadLetterStats

    /**
     * Close dead letter queue resources.
     */
    suspend fun close()
}

/**
 * Dead letter record containing the failed task and failure information.
 */
@Serializable
data class DeadLetterRecord(
    /** Unique dead letter ID */
    val id: String,

    /** Original task message */
    val task: TaskMessage,

    /** Reason for failure */
    val reason: String,

    /** Exception message */
    @SerialName("error_message")
    val errorMessage: String? = null,

    /** Exception type */
    @SerialName("error_type")
    val errorType: String? = null,

    /** Full stack trace */
    @SerialName("stack_trace")
    val stackTrace: String? = null,

    /** When the failure occurred */
    @SerialName("failed_at")
    val failedAt: Instant = Instant.now(),

    /** Number of times replayed */
    @SerialName("replay_count")
    val replayCount: Int = 0,

    /** Last replay attempt time */
    @SerialName("last_replay_at")
    val lastReplayAt: Instant? = null,

    /** Original queue */
    @SerialName("original_queue")
    val originalQueue: String = "default",

    /** Worker that failed the task */
    val worker: String? = null,

    /** Additional metadata */
    val metadata: Map<String, String> = emptyMap()
)

/**
 * Dead letter statistics.
 */
data class DeadLetterStats(
    /** Total number of dead letters */
    val totalCount: Long,

    /** Number of dead letters per task */
    val perTask: Map<String, Long>,

    /** Number of dead letters per reason */
    val perReason: Map<String, Long>,

    /** Number of dead letters per error type */
    val perErrorType: Map<String, Long>,

    /** Oldest dead letter timestamp */
    val oldestEntry: Instant?,

    /** Newest dead letter timestamp */
    val newestEntry: Instant?,

    /** Total replayed count */
    val totalReplayed: Long
)

/**
 * In-memory implementation for testing and development.
 */
class InMemoryDeadLetterQueue : DeadLetterQueue {
    private val records = mutableMapOf<String, DeadLetterRecord>()
    private val lock = java.util.concurrent.locks.ReentrantLock()

    override suspend fun enqueue(
        task: TaskMessage,
        reason: String,
        exception: Throwable?
    ) {
        lock.lock()
        try {
            val record = DeadLetterRecord(
                id = "dlq-${java.util.UUID.randomUUID()}",
                task = task,
                reason = reason,
                errorMessage = exception?.message,
                errorType = exception?.javaClass?.name,
                stackTrace = exception?.stackTraceToString(),
                failedAt = Instant.now(),
                originalQueue = task.queue
            )
            records[record.id] = record
        } finally {
            lock.unlock()
        }
    }

    override suspend fun replay(deadLetterId: String): Boolean {
        lock.lock()
        try {
            val record = records[deadLetterId] ?: return false
            records[deadLetterId] = record.copy(
                replayCount = record.replayCount + 1,
                lastReplayAt = Instant.now()
            )
            return true
        } finally {
            lock.unlock()
        }
    }

    override suspend fun replayAll(deadLetterIds: List<String>): Int {
        return deadLetterIds.count { replay(it) }
    }

    override suspend fun replayByTask(taskName: String, maxCount: Int): Int {
        lock.lock()
        try {
            return records.values
                .filter { it.task.taskName == taskName }
                .take(maxCount)
                .count { replay(it.id) }
        } finally {
            lock.unlock()
        }
    }

    override suspend fun get(deadLetterId: String): DeadLetterRecord? {
        return records[deadLetterId]
    }

    override suspend fun list(
        taskName: String?,
        limit: Int,
        offset: Int
    ): List<DeadLetterRecord> {
        lock.lock()
        try {
            return records.values
                .let { if (taskName != null) it.filter { r -> r.task.taskName == taskName } else it }
                .sortedByDescending { it.failedAt }
                .drop(offset)
                .take(limit)
        } finally {
            lock.unlock()
        }
    }

    override fun stream(taskName: String?): Flow<DeadLetterRecord> {
        return kotlinx.coroutines.flow.flow {
            records.values
                .let { if (taskName != null) it.filter { r -> r.task.taskName == taskName } else it }
                .sortedByDescending { it.failedAt }
                .forEach { emit(it) }
        }
    }

    override suspend fun delete(deadLetterId: String) {
        records.remove(deadLetterId)
    }

    override suspend fun deleteByTask(taskName: String): Int {
        lock.lock()
        try {
            val toRemove = records.values.filter { it.task.taskName == taskName }
            toRemove.forEach { records.remove(it.id) }
            return toRemove.size
        } finally {
            lock.unlock()
        }
    }

    override suspend fun purge(olderThan: Duration): Int {
        lock.lock()
        try {
            val cutoff = Instant.now().minusMillis(olderThan.inWholeMilliseconds)
            val toRemove = records.values.filter { it.failedAt.isBefore(cutoff) }
            toRemove.forEach { records.remove(it.id) }
            return toRemove.size
        } finally {
            lock.unlock()
        }
    }

    override suspend fun count(taskName: String?): Long {
        lock.lock()
        try {
            return records.values
                .let { if (taskName != null) it.filter { r -> r.task.taskName == taskName } else it }
                .count()
                .toLong()
        } finally {
            lock.unlock()
        }
    }

    override suspend fun getStats(): DeadLetterStats {
        lock.lock()
        try {
            return DeadLetterStats(
                totalCount = records.size.toLong(),
                perTask = records.values.groupBy { it.task.taskName }
                    .mapValues { it.value.size.toLong() },
                perReason = records.values.groupBy { it.reason }
                    .mapValues { it.value.size.toLong() },
                perErrorType = records.values.groupBy { it.errorType ?: "unknown" }
                    .mapValues { it.value.size.toLong() },
                oldestEntry = records.values.minByOrNull { it.failedAt }?.failedAt,
                newestEntry = records.values.maxByOrNull { it.failedAt }?.failedAt,
                totalReplayed = records.values.sumOf { it.replayCount.toLong() }
            )
        } finally {
            lock.unlock()
        }
    }

    override suspend fun close() {
        records.clear()
    }
}