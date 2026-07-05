package io.celery.task

import io.celery.deadletter.InstantSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.decodeFromJsonElement
import java.time.Instant

/**
 * Execution context passed to tasks during execution.
 * Contains all metadata and payload needed for task execution.
 */
@Serializable
data class TaskContext(
    /** Unique task execution ID */
    val taskId: String,

    /** Registered task name */
    val taskName: String,

    /** When the task was scheduled/originated */
    @Serializable(with = InstantSerializer::class)
    val originTime: Instant = Instant.now(),

    /** When the task execution started */
    @Serializable(with = InstantSerializer::class)
    val executionTime: Instant = Instant.now(),

    /** Current retry attempt (0-based) */
    val attempt: Int = 0,

    /** Whether this execution is due to misfire */
    val isMisfire: Boolean = false,

    /** Whether this is a scheduled (vs async) execution */
    val isScheduled: Boolean = false,

    /** Worker ID executing this task */
    val workerName: String? = null,

    /** Queue name the task was consumed from */
    val queue: String = "default",

    /** Task priority (lower = higher priority) */
    val priority: Int = 0,

    /** Serialized task arguments */
    val args: List<JsonElement> = emptyList(),

    /** Serialized task keyword arguments */
    val kwargs: Map<String, JsonElement> = emptyMap(),

    /** Arbitrary headers/metadata */
    val headers: Map<String, String> = emptyMap(),

    /** When the task expires (null = no expiry) */
    @Serializable(with = InstantSerializer::class)
    val expiresAt: Instant? = null,

    /** Parent task ID if this is a child task */
    val parentId: String? = null,

    /** Correlation ID for tracing */
    val correlationId: String? = null
) {
    /**
     * Get a string value from kwargs or args.
     */
    fun getString(key: String): String? {
        return kwargs[key]?.let {
            kotlinx.serialization.json.Json.decodeFromJsonElement<String>(it)
        } ?: args.firstOrNull()?.let {
            kotlinx.serialization.json.Json.decodeFromJsonElement<String>(it)
        }
    }

    /**
     * Get a typed value from kwargs.
     */
    inline fun <reified T> getValue(key: String): T? {
        return kwargs[key]?.let {
            kotlinx.serialization.json.Json.decodeFromJsonElement<T>(it)
        }
    }

    /**
     * Get a required string value, throws if missing.
     */
    fun requireString(key: String): String {
        return getString(key) ?: throw IllegalArgumentException(
            "Missing required field: $key in task $taskName"
        )
    }

    /**
     * Check if the task has expired.
     */
    fun isExpired(): Boolean {
        return expiresAt?.isBefore(Instant.now()) == true
    }
}