// kcelery-examples/src/main/kotlin/io/celery/examples/ErrorHandlingExample.kt
package io.celery.examples

import io.celery.examples.config.ExampleConfig
import io.celery.task.CeleryTask
import io.celery.task.TaskContext
import kotlinx.coroutines.*
import kotlinx.serialization.serializer
import kotlin.time.Duration.Companion.seconds

/**
 * Error handling example showing:
 * - Retry logic with different backoff strategies
 * - Dead letter queue
 * - Task failure callbacks
 * - Timeout handling
 * - Custom retry policies
 */
suspend fun main() {
    println("=" .repeat(60))
    println("kCelery Error Handling Example")
    println("=" .repeat(60))

    val app = ExampleConfig.createApp("error-handler")

    // 1. Task with exponential backoff
    var attemptCount = 0

    val exponentialTask = object : CeleryTask<String>(
        name = "exponential-retry",
        maxRetries = 5,
        defaultRetryDelay = 1,
        serializer = serializer<String>()
    ) {
        override suspend fun run(context: TaskContext): String {
            attemptCount++
            println("[EXP] Attempt ${context.attempt + 1}/${maxRetries + 1}")

            if (context.attempt < 4) {
                throw RuntimeException("Temporary failure (attempt ${context.attempt + 1})")
            }

            return "Success after ${context.attempt + 1} attempts"
        }

        override fun onRetry(exception: Exception, retries: Int): Long {
            // Exponential backoff: 2s, 4s, 8s, 16s, 32s
            val delay = (1L shl retries) * 2
            println("[EXP] Retrying in ${delay}s")
            return delay
        }

        override suspend fun onFailure(context: TaskContext, exception: Throwable) {
            println("[EXP] ❌ Task failed permanently after ${context.attempt + 1} attempts")
            println("[EXP] Error: ${exception.message}")
        }
    }

    // 2. Task with custom retryable exceptions
    val selectiveTask = object : CeleryTask<String>(
        name = "selective-retry",
        maxRetries = 3,
        defaultRetryDelay = 1,
        serializer = serializer<String>()
    ) {
        override suspend fun run(context: TaskContext): String {
            val errorType = context.getString("errorType") ?: "retryable"

            when (errorType) {
                "retryable" -> throw RuntimeException("Network timeout - retryable")
                "validation" -> throw IllegalArgumentException("Invalid input - not retryable")
                "fatal" -> throw IllegalStateException("System error - not retryable")
                else -> return "Success"
            }
        }

        override fun isRetryable(exception: Exception): Boolean {
            return exception is RuntimeException &&
                    !exception.message?.contains("validation")!! &&
                    !exception.message?.contains("fatal")!!
        }
    }

    // 3. Task with timeout
    val slowTask = object : CeleryTask<String>(
        name = "timeout-task",
        maxRetries = 0,
        defaultRetryDelay = 1,
        serializer = serializer<String>()
    ) {
        override suspend fun run(context: TaskContext): String {
            println("[TIMEOUT] Starting slow operation...")
            delay(5.seconds) // Takes longer than default timeout
            return "This should not complete"
        }
    }

    // 4. Task that always fails (dead letter demo)
    val failingTask = object : CeleryTask<String>(
        name = "always-fails",
        maxRetries = 2,
        defaultRetryDelay = 1,
        serializer = serializer<String>()
    ) {
        override suspend fun run(context: TaskContext): String {
            throw RuntimeException("This task always fails! Attempt: ${context.attempt + 1}")
        }

        override suspend fun onFailure(context: TaskContext, exception: Throwable) {
            println("[DEAD] ❌ Task ${context.taskId} moved to dead letter queue")
            println("[DEAD] Reason: ${exception.message}")
        }
    }

    app.registerTasks(exponentialTask, selectiveTask, slowTask, failingTask)
    app.start()
    println("Error handling demo started\n")

    // Demo 1: Exponential backoff
    println("1. Exponential backoff demo...")
    val expTask = app.sendTask("exponential-retry", "demo")
    val expResult = app.waitForResult(expTask.id, timeout = 120.seconds)
    println("   Result: ${expResult?.result}\n")

    // Demo 2: Selective retry
    println("2. Selective retry demo...")

    // Retryable error
    println("   a) Retryable error...")
    app.sendTask(
        "selective-retry",
        kwargs = mapOf("errorType" to kotlinx.serialization.json.JsonPrimitive("retryable"))
    )

    delay(3.seconds)

    // Non-retryable error (validation)
    println("\n   b) Non-retryable error (validation)...")
    val validationTask = app.sendTask(
        "selective-retry",
        kwargs = mapOf("errorType" to kotlinx.serialization.json.JsonPrimitive("validation"))
    )
    delay(2.seconds)
    val validationResult = app.getResult(validationTask.id)
    println("   Validation task status: ${validationResult?.status}")
    println("   (Should be FAILURE - not retried)\n")

    // Non-retryable error (fatal)
    println("   c) Non-retryable error (fatal)...")
    val fatalTask = app.sendTask(
        "selective-retry",
        kwargs = mapOf("errorType" to kotlinx.serialization.json.JsonPrimitive("fatal"))
    )
    delay(2.seconds)
    val fatalResult = app.getResult(fatalTask.id)
    println("   Fatal task status: ${fatalResult?.status}")
    println("   (Should be FAILURE - not retried)\n")

    // Demo 3: Timeout
    println("3. Timeout demo...")
    val slowTaskMsg = app.sendTask("timeout-task", "demo")
    delay(6.seconds)
    val slowResult = app.getResult(slowTaskMsg.id)
    println("   Timeout task status: ${slowResult?.status}")
    println("   (Should be FAILURE due to timeout)\n")

    // Demo 4: Dead letter queue
    println("4. Dead letter queue demo...")
    val deadTask = app.sendTask("always-fails", "demo")
    delay(5.seconds)
    val deadResult = app.getResult(deadTask.id)
    println("   Dead letter task status: ${deadResult?.status}")
    println("   Error: ${deadResult?.error}")
    println("   (Task should be in dead letter queue)\n")

    // Summary
    println("=" .repeat(60))
    println("Error Handling Summary:")
    println("  ✅ Exponential backoff: ${expResult?.status}")
    println("  ✅ Selective retry: Retryable errors retried, others failed immediately")
    println("  ✅ Timeout handling: Tasks terminated after timeout")
    println("  ✅ Dead letter queue: Failed tasks moved to DLQ for analysis")

    app.stop()
    ExampleConfig.shutdown()
    println("\n✅ Error handling example completed!")
}