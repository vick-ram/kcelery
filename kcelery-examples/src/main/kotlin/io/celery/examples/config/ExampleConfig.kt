package io.celery.examples.config

import io.celery.CeleryApp
import io.celery.redis.RedisConfig
import io.celery.redis.RedisConnectionFactory
import io.celery.redis.RedisDeadLetterQueue
import io.celery.redis.RedisDistributedLock
import io.celery.redis.RedisLeaderElector
import io.celery.redis.RedisMessageBroker
import io.celery.redis.RedisResultBackend
import org.slf4j.LoggerFactory
import io.lettuce.core.RedisClient
import kotlinx.serialization.json.Json
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

/**
 * Shared configuration for all examples.
 * Uses TestContainers to spin up a Redis instance automatically.
 */
object ExampleConfig {
    private val logger = LoggerFactory.getLogger(javaClass)

    // Redis container for examples
    private val redisContainer: GenericContainer<*> by lazy {
        logger.info("Starting Redis container for examples...")
        GenericContainer(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379)
            .apply { start() }
    }

    /**
     * Get Redis URL from test container.
     */
    fun getRedisUrl(): String {
        val container = redisContainer
        return "redis://${container.host}:${container.firstMappedPort}"
    }

    /**
     * Create a fully configured CeleryApp for examples.
     */
    fun createApp(name: String = "example-app"): CeleryApp {
        val redisUrl = getRedisUrl()
        val redisClient = RedisClient.create(redisUrl)

        val connectionFactory = RedisConnectionFactory(
            RedisConfig.Standalone(url = redisUrl)
        )

        val broker = RedisMessageBroker(connectionFactory)
        val backend = RedisResultBackend(connectionFactory)
        val lock = RedisDistributedLock(redisClient)
        val leaderElector = RedisLeaderElector(redisClient, lock)
        val deadLetterQueue = RedisDeadLetterQueue(connectionFactory)

        return CeleryApp.builder()
            .withName(name)
            .withBroker(broker)
            .withBackend(backend)
            .withLock(lock)
            .withLeaderElector(leaderElector)
            .withDeadLetterQueue(deadLetterQueue)
            .withWorkerCount(min = 2, max = 4)
            .withWorkerConcurrency(2)
            .withJson(Json {
                ignoreUnknownKeys = true
                prettyPrint = true
            })
            .build()
    }

    /**
     * Shutdown Redis container.
     */
    fun shutdown() {
        redisContainer.stop()
        logger.info("Redis container stopped")
    }
}