package io.celery.test

import io.celery.redis.RedisDistributedLockManager
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.config.Config
import org.testcontainers.containers.GenericContainer
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName

@OptIn(ExperimentalCoroutinesApi::class)
@Testcontainers
abstract class RedisTestBase : TestBase() {

    companion object {
        @Container
        val redisContainer = GenericContainer(
            DockerImageName.parse("redis:7-alpine")
        ).apply {
            withExposedPorts(6379)
        }
    }

    protected lateinit var redisClient: RedisClient
    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    protected lateinit var redisCommands: RedisCoroutinesCommands<String, String>
    protected lateinit var redissonClient: RedissonClient
    protected lateinit var lockManager: RedisDistributedLockManager
    protected lateinit var redisUrl: String

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    @BeforeEach
    override fun setUp() {
        super.setUp()

        redisUrl = "redis://${redisContainer.host}:${redisContainer.firstMappedPort}"

        redisClient = RedisClient.create(redisUrl)
        redisCommands = redisClient.connect().coroutines()

        val redissonConfig = Config().apply {
            useSingleServer().address = redisUrl
        }
        redissonClient = Redisson.create(redissonConfig)

        lockManager = RedisDistributedLockManager(redisClient)
    }

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    @AfterEach
    override fun tearDown() {
        super.tearDown()

        runTest {
            redisCommands.flushall()
        }

        redisClient.shutdown()
        redissonClient.shutdown()
        lockManager.close()
    }
}