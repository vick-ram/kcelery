package io.celery

import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.JsonElement
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.UUID

abstract class CeleryTask<T : Any>(
    val name: String,
    val maxRetries: Int = 3,
    val defaultRetryDelay: Long = 60,
    val serializer: KSerializer<out Any>
) {
    // This is the primary method that consumers will implement
    abstract suspend fun run(context: TaskContext): T

    // Provide a default implementation for the args/kwargs version
    // This will create a TaskContext and delegate to the primary run method
    open suspend fun run(args: List<JsonElement>, kwargs: Map<String, JsonElement>): T {
        val context = TaskContext(
            taskId = UUID.randomUUID().toString(), // Generate a new ID for async calls
            taskName = name,
            args = args,
            kwargs = kwargs,
            executionTime = Instant.now(), // Use current time for async calls
            attempt = 0,
            isMisfire = false,
            isScheduled = false
        )
        return run(context)
    }

    open fun onRetry(exc: Exception, retries: Int): Long {
        // Exponential backoff by default
        return defaultRetryDelay * (1L shl (retries - 1))
    }

    open fun onFailure(exc: Exception) {
        LoggerFactory.getLogger(javaClass).error("Task $name failed permanently", exc)
    }
}

data class TaskContext(
    val taskId: String,
    val taskName: String,
    val args: List<JsonElement>,
    val kwargs: Map<String, JsonElement>,
    val executionTime: Instant,
    val attempt: Int,
    val isMisfire: Boolean,
    val isScheduled: Boolean,
    val workerName: String? = null,
    val queue: String = "default"
)