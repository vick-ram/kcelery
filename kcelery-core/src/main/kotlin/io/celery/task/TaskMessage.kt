package io.celery.task

import io.celery.deadletter.InstantSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerialName
import kotlinx.serialization.json.JsonElement
import java.time.Instant

/**
 * Serialized envelope for task messages.
 * This is what gets sent over the broker.
 */
@Serializable
data class TaskMessage(
    /** Unique message/execution ID */
    val id: String,

    /** Registered task name */
    val taskName: String,

    /** Task arguments (serialized) */
    val args: List<JsonElement> = emptyList(),

    /** Task keyword arguments (serialized) */
    val kwargs: Map<String, JsonElement> = emptyMap(),

    /** Task priority (lower = higher priority) */
    val priority: Int = 0,

    /** Number of retries attempted so far */
    val retries: Int = 0,

    /** Maximum retries allowed */
    val maxRetries: Int = 3,

    /** Epoch millis when task should execute */
    @SerialName("eta")
    val eta: Long? = null,

    /** Epoch millis when task expires */
    @SerialName("expires")
    val expires: Long? = null,

    /** Target queue name */
    val queue: String = "default",

    /** Origin application name */
    val origin: String? = null,

    /** Arbitrary headers */
    val headers: Map<String, String> = emptyMap(),

    /** When the message was created */
    @SerialName("created_at")
    @Serializable(with = InstantSerializer::class)
    val createdAt: Instant = Instant.now(),

    /** Parent task ID for task chains */
    @SerialName("parent_id")
    val parentId: String? = null,

    /** Root task ID for task groups */
    @SerialName("root_id")
    val rootId: String? = null,

    /** Correlation ID for distributed tracing */
    @SerialName("correlation_id")
    val correlationId: String? = null
) {
    /** Check if this message has expired */
    fun isExpired(): Boolean {
        return expires?.let { Instant.now().toEpochMilli() > it } == true
    }

    /** Check if this message is ready to execute */
    fun isReady(): Boolean {
        return eta?.let { Instant.now().toEpochMilli() >= it } != false
    }

    /** Convert to TaskContext for execution */
    fun toTaskContext(workerName: String? = null): TaskContext {
        return TaskContext(
            taskId = id,
            taskName = taskName,
            originTime = createdAt,
            attempt = retries,
            queue = queue,
            priority = priority,
            args = args,
            kwargs = kwargs,
            headers = headers,
            expiresAt = expires?.let { Instant.ofEpochMilli(it) },
            parentId = parentId,
            correlationId = correlationId,
            workerName = workerName
        )
    }

    companion object {
        /**
         * Create a TaskMessage from components.
         */
        fun create(
            taskName: String,
            args: List<JsonElement> = emptyList(),
            kwargs: Map<String, JsonElement> = emptyMap(),
            priority: Int = 0,
            queue: String = "default",
            delay: kotlin.time.Duration? = null,
            expires: kotlin.time.Duration? = null,
            headers: Map<String, String> = emptyMap(),
            id: String = java.util.UUID.randomUUID().toString()
        ): TaskMessage {
            val now = Instant.now()
            return TaskMessage(
                id = id,
                taskName = taskName,
                args = args,
                kwargs = kwargs,
                priority = priority,
                queue = queue,
                eta = delay?.let { now.toEpochMilli() + it.inWholeMilliseconds },
                expires = expires?.let { now.toEpochMilli() + it.inWholeMilliseconds },
                headers = headers,
                createdAt = now
            )
        }
    }
}