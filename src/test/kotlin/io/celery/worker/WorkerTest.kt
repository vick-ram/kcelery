package io.celery.worker

import io.celery.core.SerializerRegistry
import io.celery.model.*
import io.celery.redis.MessageBroker
import io.celery.redis.RedisBackend
import io.celery.redis.RedisBroker
import io.celery.redis.ResultBackend
import io.celery.test.RedisTestBase
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class WorkerTest : RedisTestBase() {

    private lateinit var broker: MessageBroker
    private lateinit var backend: ResultBackend
    private lateinit var taskRegistry: TaskRegistry
    private lateinit var serializerRegistry: SerializerRegistry

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    @BeforeEach
    override fun setUp() {
        super.setUp()

        broker = RedisBroker(redisCommands, json)
        backend = RedisBackend(redisCommands, json)
        taskRegistry = TaskRegistry()
    }

    @Test
    fun `worker should process tasks`() = runTest {
        val latch = CountDownLatch(1)
        var processedArgs: List<JsonElement>? = null

        val testTask = object : CeleryTask<String>(
            name = "process-data",
            serializer = kotlinx.serialization.serializer()
        ) {
            override suspend fun run(context: TaskContext): String {
                TODO("Not yet implemented")
            }

            override suspend fun run(
                args: List<JsonElement>,
                kwargs: Map<String, JsonElement>
            ): String {
                processedArgs = args
                latch.countDown()
                return "processed"
            }
        }

        taskRegistry.register(testTask)

        val worker = Worker(
            name = "test-worker",
            queues = listOf("default"),
            concurrency = 1,
            broker = broker,
            backend = backend,
            taskRegistry = taskRegistry,
            serializerRegistry = serializerRegistry
        )

        worker.start()

        broker.publish(
            TaskMessage(
                taskName = "process-data",
                args = listOf(JsonPrimitive("input.txt"))
            ),
            "default"
        )

        assertTrue(latch.await(5, TimeUnit.SECONDS))
        assertEquals(1, processedArgs?.size)
        assertEquals("input.txt", processedArgs?.first()?.jsonPrimitive?.content)

        worker.stop()
    }

    @Test
    fun `worker should handle task failure and retry`() = runTest {
        val executionCount = java.util.concurrent.atomic.AtomicInteger(0)
        val maxRetries = 2

        val flakyTask = object : CeleryTask<String>(
            name = "flaky",
            maxRetries = maxRetries,
            defaultRetryDelay = 1,
            serializer = kotlinx.serialization.serializer()
        ) {
            override suspend fun run(context: TaskContext): String {
                TODO("Not yet implemented")
            }

            override suspend fun run(
                args: List<JsonElement>,
                kwargs: Map<String, JsonElement>
            ): String {
                executionCount.incrementAndGet()
                throw RuntimeException("Simulated failure")
            }
        }

        taskRegistry.register(flakyTask)

        val worker = Worker(
            name = "test-worker",
            queues = listOf("default"),
            concurrency = 1,
            broker = broker,
            backend = backend,
            taskRegistry = taskRegistry,
            serializerRegistry = serializerRegistry
        )

        worker.start()

        broker.publish(
            TaskMessage(
                taskName = "flaky",
                maxRetries = maxRetries
            ),
            "default"
        )

        delay(5.seconds)

        assertTrue(executionCount.get() > 1, "Task should have been retried")

        worker.stop()
    }

    @Test
    fun `worker should store results in backend`() = runTest {
        val testTask = object : CeleryTask<String>(
            name = "store-result",
            serializer = kotlinx.serialization.serializer()
        ) {
            override suspend fun run(context: TaskContext): String {
                TODO("Not yet implemented")
            }

            override suspend fun run(
                args: List<JsonElement>,
                kwargs: Map<String, JsonElement>
            ): String {
                return "success-result"
            }
        }

        taskRegistry.register(testTask)

        val worker = Worker(
            name = "test-worker",
            queues = listOf("default"),
            concurrency = 1,
            broker = broker,
            backend = backend,
            taskRegistry = taskRegistry,
            serializerRegistry = serializerRegistry
        )

        worker.start()

        val message = TaskMessage(taskName = "store-result")
        broker.publish(message, "default")

        delay(3.seconds)

        val result = backend.getResult(message.id)
        assertNotNull(result)
        assertEquals(TaskState.SUCCESS, result?.state)

        worker.stop()
    }

    @Test
    fun `worker should handle unknown task`() = runTest {
        val worker = Worker(
            name = "test-worker",
            queues = listOf("default"),
            concurrency = 1,
            broker = broker,
            backend = backend,
            taskRegistry = taskRegistry,
            serializerRegistry = serializerRegistry
        )

        worker.start()

        broker.publish(
            TaskMessage(taskName = "non-existent-task"),
            "default"
        )

        delay(2.seconds)

        // Should not crash, just reject unknown task
        assertDoesNotThrow {
            worker.stop()
        }
    }

    @Test
    fun `worker should respect concurrency limits`() = runTest {
        val maxConcurrent = java.util.concurrent.atomic.AtomicInteger(0)
        val currentConcurrent = java.util.concurrent.atomic.AtomicInteger(0)
        val concurrency = 2

        val slowTask = object : CeleryTask<String>(
            name = "slow-task",
            serializer = kotlinx.serialization.serializer()
        ) {
            override suspend fun run(context: TaskContext): String {
                TODO("Not yet implemented")
            }

            override suspend fun run(
                args: List<JsonElement>,
                kwargs: Map<String, JsonElement>
            ): String {
                val current = currentConcurrent.incrementAndGet()
                maxConcurrent.updateAndGet { maxOf(it, current) }
                delay(500.milliseconds) // Simulate work
                currentConcurrent.decrementAndGet()
                return "done"
            }
        }

        taskRegistry.register(slowTask)

        val worker = Worker(
            name = "test-worker",
            queues = listOf("default"),
            concurrency = concurrency,
            broker = broker,
            backend = backend,
            taskRegistry = taskRegistry,
            serializerRegistry = serializerRegistry
        )

        worker.start()

        // Publish multiple tasks quickly
        repeat(5) {
            broker.publish(TaskMessage(taskName = "slow-task"), "default")
        }

        delay(3.seconds)

        assertTrue(maxConcurrent.get() <= concurrency,
            "Should not exceed concurrency limit of $concurrency, got ${maxConcurrent.get()}")

        worker.stop()
    }

    @Test
    fun `worker should revoke task`() = runTest {
        val started = CountDownLatch(1)
        val completed = java.util.concurrent.atomic.AtomicBoolean(false)

        val longTask = object : CeleryTask<String>(
            name = "long-task",
            serializer = kotlinx.serialization.serializer()
        ) {
            override suspend fun run(context: TaskContext): String {
                TODO("Not yet implemented")
            }

            override suspend fun run(
                args: List<JsonElement>,
                kwargs: Map<String, JsonElement>
            ): String {
                started.countDown()
                delay(5000.milliseconds)
                completed.set(true)
                return "done"
            }
        }

        taskRegistry.register(longTask)

        val worker = Worker(
            name = "test-worker",
            queues = listOf("default"),
            concurrency = 1,
            broker = broker,
            backend = backend,
            taskRegistry = taskRegistry,
            serializerRegistry = serializerRegistry
        )

        worker.start()

        val message = TaskMessage(taskName = "long-task")
        broker.publish(message, "default")

        assertTrue(started.await(2, TimeUnit.SECONDS))

        val revoked = worker.revokeTask(message.id)
        assertTrue(revoked, "Task should be revoked")

        delay(2.seconds)

        assertFalse(completed.get(), "Task should not complete after revocation")

        worker.stop()
    }
}