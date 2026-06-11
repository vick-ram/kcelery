package io.celery.model

import io.celery.config.InstantSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import java.time.Instant
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

@Serializable
data class TaskMessage @OptIn(ExperimentalUuidApi::class) constructor(
    val id: String = Uuid.generateV7().toString(),
    val taskName: String,
    val args: List<JsonElement> = emptyList(),
    val kwargs: Map<String, JsonElement> = emptyMap(),
    val retries: Int = 0,
    val maxRetries: Int = 3,
    val retryDelay: Long = 60, // seconds
    val eta: Long? = null, // epoch millis when task should execute
    val expires: Long? = null, // epoch millis when task expires
    val priority: Int = 0,
    val queue: String = "default",
    val origin: String? = null
)

@Serializable
data class TaskResult(
    val taskId: String,
    val state: TaskState,
    val result: JsonElement? = null,
    val error: String? = null,
    val traceback: String? = null,
    @Serializable(with = InstantSerializer::class)
    val executionTime: Instant? = null,
    @Serializable(with = InstantSerializer::class)
    val completedAt: Instant? = null,
    val workerName: String? = null,
    @Serializable(with = InstantSerializer::class)
    val nextScheduledRun: Instant? = null,
    val scheduleInfo: ScheduleInfo? = null
)

enum class TaskState {
    PENDING, SCHEDULED, STARTED, RUNNING, PAUSED, CANCELLED, SUCCESS, FAILURE, RETRY, REVOKED, EXPIRED
}

@Serializable
data class ScheduleInfo(
    val scheduleType: String, // "cron", "fixed_delay", "fixed_rate"
    val expression: String? = null,
    val intervalMs: Long? = null
)