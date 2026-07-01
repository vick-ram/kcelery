// kcelery-core/src/main/kotlin/io/celery/backend/ResultStatus.kt
package io.celery.backend

/**
 * Task execution status.
 */
enum class ResultStatus {
    /** Task is pending execution */
    PENDING,

    /** Task has been received by worker */
    RECEIVED,

    /** Task has started executing */
    STARTED,

    /** Task is currently retrying */
    RETRY,

    /** Task completed successfully */
    SUCCESS,

    /** Task failed */
    FAILURE,

    /** Task was revoked/cancelled */
    REVOKED,

    /** Task expired before execution */
    EXPIRED,

    /** Task was rejected */
    REJECTED;

    /** Check if this is a terminal state */
    fun isTerminal(): Boolean = this in setOf(SUCCESS, FAILURE, REVOKED, EXPIRED, REJECTED)

    /** Check if this is a success state */
    fun isSuccess(): Boolean = this == SUCCESS

    /** Check if this is a failure state */
    fun isFailure(): Boolean = this in setOf(FAILURE, REJECTED, EXPIRED)
}