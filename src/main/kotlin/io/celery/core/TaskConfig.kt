package io.celery.core

import kotlinx.serialization.Serializable

@Serializable
data class TaskConfig(
    val allowConcurrentExecution: Boolean = false,
    val misfirePolicy: MisfirePolicy = MisfirePolicy.IGNORE,
    val maxRetries: Int = 0,
    val retryDelayMs: Long = 1000,
    val timeoutMs: Long = 30_000, // Default 30 seconds
    val maxRetryDelayMs: Long = 60_000,
    val retryBackoffMultiplier: Double = 2.0,
    val deadLetterEnabled: Boolean = true
) {
    init {
        require(maxRetries >= 0) { "maxRetries must be non-negative" }
        require(retryDelayMs > 0) { "retryDelayMs must be positive" }
        require(timeoutMs > 0) { "timeoutMs must be positive" }
        require(maxRetryDelayMs >= retryDelayMs) { "maxRetryDelayMs must be >= retryDelayMs" }
        require(retryBackoffMultiplier >= 1.0) { "retryBackoffMultiplier must be >= 1.0" }
    }
}