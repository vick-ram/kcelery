package io.celery.redis

import io.celery.backend.*
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.Range
import kotlinx.coroutines.flow.forEach
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import kotlin.time.Duration
import kotlin.time.Duration.Companion.hours

/**
 * Redis-based result backend.
 * Stores task results with TTL-based expiration.
 */
@OptIn(ExperimentalLettuceCoroutinesApi::class)
class RedisResultBackend(
    private val connectionFactory: RedisConnectionFactory,
    private val json: Json = Json { ignoreUnknownKeys = true },
    private val config: RedisResultConfig = RedisResultConfig()
) : ResultBackend {

    private val logger = LoggerFactory.getLogger(javaClass)

    /**
     * Store a task result.
     */
    override suspend fun storeResult(
        taskId: String,
        result: TaskResult,
        expiry: Duration
    ) {
        val commands = connectionFactory.getCommands()
        val key = resultKey(taskId)

        try {
            val resultJson = json.encodeToString(result)
            commands.set(key, resultJson)

            if (expiry.isPositive()) {
                commands.expire(key, expiry.inWholeSeconds)
            }

            // Also update status set for faster lookups
            if (result.status.isTerminal()) {
                commands.zadd(completedKey(),
                    result.completedAt?.toEpochMilli()?.toDouble()
                        ?: java.time.Instant.now().toEpochMilli().toDouble(),
                    taskId
                )
            }

            logger.debug("Stored result for task: $taskId (${result.status})")

        } catch (e: Exception) {
            logger.error("Failed to store result for task: $taskId", e)
            throw BackendException("Failed to store result", e)
        }
    }

    /**
     * Get a task result.
     */
    override suspend fun getResult(taskId: String): TaskResult? {
        val commands = connectionFactory.getCommands()
        val key = resultKey(taskId)

        return try {
            val resultJson = commands.get(key)
            if (resultJson != null) {
                json.decodeFromString<TaskResult>(resultJson)
            } else {
                null
            }
        } catch (e: Exception) {
            logger.error("Failed to get result for task: $taskId", e)
            null
        }
    }

    /**
     * Get task status.
     */
    override suspend fun getStatus(taskId: String): ResultStatus? {
        return getResult(taskId)?.status
    }

    /**
     * Revoke/cancel a task.
     */
    override suspend fun revokeTask(taskId: String) {
        val commands = connectionFactory.getCommands()
        val key = revokedKey(taskId)

        try {
            commands.set(key, "1")
            commands.expire(key, config.revokedTtl.inWholeSeconds)

            logger.info("Revoked task: $taskId")

        } catch (e: Exception) {
            logger.error("Failed to revoke task: $taskId", e)
        }
    }

    /**
     * Check if a task is revoked.
     */
    override suspend fun isRevoked(taskId: String): Boolean {
        val commands = connectionFactory.getCommands()
        return try {
            (commands.exists(revokedKey(taskId)) ?:0L) > 0
        } catch (e: Exception) {
            logger.error("Failed to check revoked status for task: $taskId", e)
            false
        }
    }

    /**
     * Delete a task result.
     */
    override suspend fun deleteResult(taskId: String) {
        val commands = connectionFactory.getCommands()
        try {
            commands.del(resultKey(taskId), revokedKey(taskId))
            commands.zrem(completedKey(), taskId)
            logger.debug("Deleted result for task: $taskId")
        } catch (e: Exception) {
            logger.error("Failed to delete result for task: $taskId", e)
        }
    }

    /**
     * Get results for multiple tasks.
     */
    override suspend fun getResults(taskIds: List<String>): Map<String, TaskResult?> {
        if (taskIds.isEmpty()) return emptyMap()

        val commands = connectionFactory.getCommands()
        val keys = taskIds.map(::resultKey)

        return try {
            val results = commands.mget(*keys.toTypedArray()).toList()
            taskIds.zip(results.map { kv ->
                kv.value?.let { json.decodeFromString<TaskResult>(it) }
            }).toMap()
        } catch (e: Exception) {
            logger.error("Failed to get multiple results", e)
            taskIds.associateWith { null }
        }
    }

    /**
     * Get completed tasks (for monitoring).
     */
    suspend fun getCompletedTasks(
        limit: Long = 100,
        offset: Long = 0
    ): List<String> {
        val commands = connectionFactory.getCommands()
        return try {
            commands.zrevrange(completedKey(), offset, offset + limit - 1).toList()
        } catch (e: Exception) {
            logger.error("Failed to get completed tasks", e)
            emptyList()
        }
    }

    /**
     * Clean up expired results.
     */
    suspend fun cleanupExpired(batchSize: Int = 100): Int {
        val commands = connectionFactory.getCommands()
        var cleaned = 0

        try {
            // Clean revoked keys
            val revokedKeys = commands.keys("${config.keyPrefix}:revoked:*")
            revokedKeys.collect { key ->
                commands.ttl(key)?.let {
                    if (it <= 0) {
                        commands.del(key)
                        cleaned++
                    }
                }
            }

            // Clean old completed tasks
            val cutoff = java.time.Instant.now().minusSeconds(config.resultTtl.inWholeSeconds)
            commands.zremrangebyscore(
                completedKey(),
                Range.create(
                    0L,
                    cutoff.toEpochMilli()
                )
            )

            logger.debug("Cleaned $cleaned expired results")

        } catch (e: Exception) {
            logger.error("Failed to cleanup expired results", e)
        }

        return cleaned
    }

    /**
     * Close backend.
     */
    override suspend fun close() {
        connectionFactory.close()
        logger.info("RedisResultBackend closed")
    }

    /**
     * Health check.
     */
    override suspend fun healthCheck(): Boolean {
        return connectionFactory.healthCheck()
    }

    // Key generation helpers
    private fun resultKey(taskId: String) = "${config.keyPrefix}:result:$taskId"
    private fun revokedKey(taskId: String) = "${config.keyPrefix}:revoked:$taskId"
    private fun completedKey() = "${config.keyPrefix}:completed"
}

data class RedisResultConfig(
    val keyPrefix: String = "celery",
    val resultTtl: Duration = 24.hours,
    val revokedTtl: Duration = 1.hours
)