package io.celery.task

import io.celery.scheduler.MisfirePolicy
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.minutes

/**
 * Configuration for task execution behavior.
 */
data class TaskConfig(
    /** Allow concurrent execution of the same task instance */
    val allowConcurrentExecution: Boolean = false,

    /** How to handle missed executions (scheduled tasks only) */
    val misfirePolicy: MisfirePolicy = MisfirePolicy.IGNORE,

    /** Maximum number of retries */
    val maxRetries: Int = 3,

    /** Initial retry delay */
    val retryDelay: Duration = 60.seconds,

    /** Maximum retry delay */
    val maxRetryDelay: Duration = 60.minutes,

    /** Backoff multiplier for retries */
    val retryBackoffMultiplier: Double = 2.0,

    /** Task execution timeout */
    val timeout: Duration = 5.minutes,

    /** Whether to enable dead letter queue */
    val deadLetterEnabled: Boolean = true,

    /** Whether to auto-acknowledge on completion */
    val autoAck: Boolean = true,

    /** Task priority (lower = higher priority, 0-255) */
    val priority: Int = 0,

    /** Whether the task is idempotent (safe to re-execute) */
    val idempotent: Boolean = false,

    /** Maximum number of concurrent instances across cluster */
    val maxConcurrentInstances: Int = Int.MAX_VALUE,

    /** Rate limit (max executions per second) */
    val rateLimit: Double = Double.MAX_VALUE
) {
    init {
        require(maxRetries >= 0) { "maxRetries must be non-negative, got: $maxRetries" }
        require(retryDelay.isPositive()) { "retryDelay must be positive, got: $retryDelay" }
        require(timeout.isPositive()) { "timeout must be positive, got: $timeout" }
        require(priority in 0..255) { "priority must be between 0 and 255, got: $priority" }
        require(retryBackoffMultiplier >= 1.0) {
            "retryBackoffMultiplier must be >= 1.0, got: $retryBackoffMultiplier"
        }
    }

    companion object {
        /** Default configuration suitable for most tasks */
        val DEFAULT = TaskConfig()

        /** Configuration for high-priority tasks */
        val HIGH_PRIORITY = TaskConfig(
            maxRetries = 5,
            retryDelay = 10.seconds,
            priority = 1
        )

        /** Configuration for idempotent, long-running tasks */
        val LONG_RUNNING = TaskConfig(
            timeout = 30.minutes,
            maxRetries = 1,
            idempotent = true
        )

        /** Configuration for non-critical, best-effort tasks */
        val BEST_EFFORT = TaskConfig(
            maxRetries = 0,
            deadLetterEnabled = false,
            priority = 255
        )
    }
}