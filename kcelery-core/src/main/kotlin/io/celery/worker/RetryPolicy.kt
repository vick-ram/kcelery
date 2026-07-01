package io.celery.worker

import io.celery.task.TaskConfig
import kotlinx.coroutines.TimeoutCancellationException
import kotlin.time.Duration
import kotlin.math.min
import kotlin.math.pow

/**
 * Retry policy calculator.
 */
object RetryPolicy {

    /**
     * Calculate delay for next retry attempt.
     */
    fun calculateDelay(config: TaskConfig, attempt: Int): Duration {
        val baseDelay = config.retryDelay.inWholeMilliseconds
        val maxDelay = config.maxRetryDelay.inWholeMilliseconds

        val delay = if (attempt <= 0) {
            baseDelay
        } else {
            (baseDelay * config.retryBackoffMultiplier.pow(attempt.toDouble())).toLong()
        }

        return min(delay, maxDelay).milliseconds
    }

    /**
     * Determine if task should be retried.
     */
    fun shouldRetry(config: TaskConfig, attempt: Int, exception: Exception): Boolean {
        return attempt < config.maxRetries && isRetryable(exception)
    }

    /**
     * Check if exception is retryable.
     */
    private fun isRetryable(exception: Exception): Boolean {
        return when (exception) {
            is IllegalArgumentException -> false
            is IllegalStateException -> false
            is TimeoutCancellationException -> true
            else -> true
        }
    }
}