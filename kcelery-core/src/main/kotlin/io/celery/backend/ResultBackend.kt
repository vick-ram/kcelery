package io.celery.backend

import io.celery.deadletter.InstantSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours

/**
 * Result backend for storing and retrieving task results.
 */
interface ResultBackend {
    /**
     * Store a task result.
     * 
     * @param taskId Unique task ID
     * @param result The result to store
     * @param expiry TTL for the result
     */
    suspend fun storeResult(
        taskId: String,
        result: TaskResult,
        expiry: Duration = 24.hours
    )

    /**
     * Get a task result.
     * 
     * @param taskId Unique task ID
     * @return The result, or null if not found/expired
     */
    suspend fun getResult(taskId: String): TaskResult?

    /**
     * Get task status.
     * 
     * @param taskId Unique task ID
     * @return The status, or null if not found
     */
    suspend fun getStatus(taskId: String): ResultStatus?

    /**
     * Revoke/cancel a task.
     * 
     * @param taskId Unique task ID
     */
    suspend fun revokeTask(taskId: String)

    /**
     * Check if a task is revoked.
     * 
     * @param taskId Unique task ID
     * @return true if task is revoked
     */
    suspend fun isRevoked(taskId: String): Boolean

    /**
     * Delete a task result.
     * 
     * @param taskId Unique task ID
     */
    suspend fun deleteResult(taskId: String)

    /**
     * Get results for multiple tasks.
     * 
     * @param taskIds List of task IDs
     * @return Map of task ID to result
     */
    suspend fun getResults(taskIds: List<String>): Map<String, TaskResult?>

    /**
     * Close backend connections.
     */
    suspend fun close()

    /**
     * Check backend health.
     * 
     * @return true if backend is healthy
     */
    suspend fun healthCheck(): Boolean
}

/**
 * Task result representation.
 */
@Serializable
data class TaskResult(
    /** Task ID */
    val taskId: String,

    /** Result status */
    val status: ResultStatus,

    /** Result value (serialized) */
    val result: JsonElement? = null,

    /** Error traceback if failed */
    val traceback: String? = null,

    /** Error message if failed */
    val error: String? = null,

    /** Error type if failed */
    val errorType: String? = null,

    /** When the task completed */
    @Serializable(with = InstantSerializer::class)
    val completedAt: Instant? = null,

    /** Worker that executed the task */
    val worker: String? = null,

    /** Task metadata */
    val metadata: Map<String, String> = emptyMap()
)

/**
 * Result backend exception.
 */
class BackendException(
    message: String,
    cause: Throwable? = null
) : RuntimeException(message, cause)