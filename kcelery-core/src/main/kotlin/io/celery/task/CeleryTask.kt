package io.celery.task

import kotlinx.serialization.KSerializer
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Base class for all Celery tasks.
 *
 * @param T The return type of the task
 * @param name Unique task name used for registration and routing
 * @param maxRetries Maximum number of retry attempts (default: 3)
 * @param defaultRetryDelay Default delay between retries in seconds (default: 60)
 * @param serializer KSerializer for the return type
 * @param autoAck Whether to automatically acknowledge messages (default: true)
 */
abstract class CeleryTask<T : Any>(
    val name: String,
    val maxRetries: Int = 3,
    val defaultRetryDelay: Long = 60,
    val executionTimeout: Duration = 10.minutes,
    val serializer: KSerializer<T>,
    val autoAck: Boolean = true
) {
    protected val logger = LoggerFactory.getLogger(CeleryTask::class.java)

    /**
     * Execute the task with the given context.
     *
     * @param context Task execution context containing payload, metadata, and execution info
     * @return The task result
     */
    abstract suspend fun run(context: TaskContext): T

    /**
     * Callback when task succeeds.
     * Override to add custom success handling (e.g., metrics, logging).
     *
     * @param context The task context
     * @param result The task result
     */
    open suspend fun onSuccess(context: TaskContext, result: T) {
        logger.debug("Task {} completed successfully", name)
    }

    /**
     * Callback when task fails.
     * Override to add custom failure handling (e.g., alerts, cleanup).
     *
     * @param context The task context
     * @param exception The exception that caused the failure
     */
    open suspend fun onFailure(context: TaskContext, exception: Throwable) {
        logger.error("Task {} failed after {} retries", name, maxRetries, exception)
    }

    /**
     * Calculate retry delay based on attempt number.
     * Override to implement custom backoff strategies.
     *
     * @param exception The exception that caused the retry
     * @param retries Current retry count (1-based)
     * @return Delay in seconds before next retry
     */
    open fun onRetry(exception: Exception, retries: Int): Long {
        return when {
            retries <= 1 -> defaultRetryDelay
            retries <= 3 -> defaultRetryDelay * retries
            else -> minOf(defaultRetryDelay * retries, 3600) // Max 1 hour
        }
    }

    /**
     * Determine if an exception is retryable.
     * Override to filter which exceptions trigger retries.
     *
     * @param exception The exception to check
     * @return true if the task should be retried
     */
    open fun isRetryable(exception: Exception): Boolean {
        return exception !is IllegalArgumentException &&
                exception !is IllegalStateException
    }

    /**
     * Called before task execution.
     * Override for setup/pre-processing logic.
     *
     * @param context The task context
     */
    open suspend fun beforeRun(context: TaskContext) {
        logger.debug("Starting task {} [{}]", name, context.taskId)
    }

    /**
     * Called after task execution (success or failure).
     * Override for cleanup logic.
     *
     * @param context The task context
     */
    open suspend fun afterRun(context: TaskContext) {
        logger.debug("Finished task {} [{}]", name, context.taskId)
    }

    override fun toString(): String = "CeleryTask(name=$name)"
}