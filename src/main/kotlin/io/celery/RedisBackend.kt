package io.celery

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.serialization.json.Json

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class RedisBackend(
    private val redis: RedisCoroutinesCommands<String, String>,
    private val json: Json,
    private val prefix: String = "celery"
) : ResultBackend {

    override suspend fun storeResult(taskId: String, result: TaskResult, expirySeconds: Long) {
        val key = "$prefix:result:$taskId"
        redis.set(key, json.encodeToString(result))
        redis.expire(key, expirySeconds)
    }

    override suspend fun getResult(taskId: String): TaskResult? {
        val key = "$prefix:result:$taskId"
        return redis.get(key)?.let { json.decodeFromString(it) }
    }

    override suspend fun revokeTask(taskId: String) {
        val key = "$prefix:revoked:$taskId"
        redis.set(key, "1")
        redis.expire(key, 3600)
    }

    override suspend fun close() {
        // Redis managed externally
    }
}