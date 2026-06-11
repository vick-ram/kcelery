package io.celery.integration

import io.celery.test.RedisTestBase
import io.celery.*
import io.celery.model.CeleryTask
import io.celery.model.TaskContext
import io.celery.test.TestBase
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class EndToEndTest : RedisTestBase() {

    @Serializable
    data class ProcessingResult(val file: String, val lines: Int)

    @Test
    fun `full workflow with scheduled and async tasks`() = runTest {
        val app = CeleryApp(
            name = "integration-test",
            redisUrl = "redis://${redisContainer.host}:${redisContainer.firstMappedPort}",
            workerThreads = 2
        )

        // Define task
        val completionLatch = CountDownLatch(2) // One for scheduled, one for async
        val processingResults = mutableListOf<ProcessingResult>()

        val fileProcessor = object : CeleryTask<ProcessingResult>(
            name = "process-file",
            maxRetries = 2,
            defaultRetryDelay = 1,
            serializer = ProcessingResult.serializer()
        ) {
            override suspend fun run(
                context: TaskContext
            ): ProcessingResult {
                val filename = context.args.firstOrNull()?.jsonPrimitive?.content
                    ?: context.kwargs["file"]?.jsonPrimitive?.content
                    ?: "unknown"

                // Simulate processing
                val lines = if (context.attempt > 0) 100 else throw RuntimeException("First attempt fails")

                val result = ProcessingResult(filename, lines)
                processingResults.add(result)
                completionLatch.countDown()

                return result
            }

            override fun onRetry(exc: Exception, retries: Int): Long = 1 // Quick retry for test
        }

        app.registerTask(fileProcessor)
        app.start(workerCount = 1, workerConcurrency = 2)

        // Schedule recurring task
        app.scheduleCron(
            id = "scheduled-process",
            taskName = "process-file",
            cronExpression = "*/1 * * * * *", // Every second
            kwargs = mapOf("file" to JsonPrimitive("scheduled-data.csv"))
        )

        // Send async task
        val asyncResult = app.sendTask(
            taskName = "process-file",
            args = listOf(JsonPrimitive("async-data.csv")),
            priority = 1
        )
        assertTrue(asyncResult.isSuccess, "Async task should be sent successfully")

        // Wait for completion
        assertTrue(
            completionLatch.await(15, TimeUnit.SECONDS),
            "Tasks should complete within timeout"
        )

        assertTrue(processingResults.isNotEmpty(), "Should have processing results")

        // Check result
        val taskId = asyncResult.getOrThrow().id
        val result = app.getResult(taskId)
        assertNotNull(result, "Should have result for async task")

        // Cleanup
        app.shutdown(Duration.ofSeconds(10))
    }

    @Test
    fun `task failure and retry workflow`() = runTest {
        val app = CeleryApp(
            name = "retry-test",
            redisUrl = "redis://${redisContainer.host}:${redisContainer.firstMappedPort}",
            workerThreads = 1
        )

        val failureLatch = CountDownLatch(1)
        val attempts = java.util.concurrent.atomic.AtomicInteger(0)

        val alwaysFails = object : CeleryTask<String>(
            name = "always-fails",
            maxRetries = 3,
            defaultRetryDelay = 1,
            serializer = kotlinx.serialization.serializer()
        ) {
            override suspend fun run(
                context: TaskContext
            ): String {
                attempts.incrementAndGet()
                throw RuntimeException("Always fails")
            }

            override fun onFailure(exc: Exception) {
                failureLatch.countDown()
            }
        }

        app.registerTask(alwaysFails)
        app.start(workerCount = 1, workerConcurrency = 1)

        app.sendTask(taskName = "always-fails")

        assertTrue(failureLatch.await(10, TimeUnit.SECONDS))
        assertTrue(attempts.get() >= 3, "Should have attempted at least 3 times")

        app.shutdown(Duration.ofSeconds(5))
    }
}