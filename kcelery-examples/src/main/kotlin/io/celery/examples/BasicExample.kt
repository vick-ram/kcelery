package io.celery.examples

import io.celery.examples.config.ExampleConfig
import io.celery.task.CeleryTask
import io.celery.task.TaskContext
import kotlinx.coroutines.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonPrimitive
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Basic example showing:
 * - Task registration
 * - Sending async tasks
 * - Getting results
 * - Waiting for completion
 */
suspend fun main() {
    println("=" .repeat(60))
    println("kCelery Basic Example")
    println("=" .repeat(60))

    // Create app
    val app = ExampleConfig.createApp("basic-example")

    // Define a simple task
    @Serializable
    data class GreetingResult(val message: String, val timestamp: Long)

    val greetingTask = object : CeleryTask<GreetingResult>(
        name = "greet",
        maxRetries = 2,
        defaultRetryDelay = 5,
        serializer = GreetingResult.serializer()
    ) {
        override suspend fun run(context: TaskContext): GreetingResult {
            val name = context.getString("name") ?: "World"
            val greeting = "Hello, $name!"

            println("[${context.taskId}] $greeting")
            delay(500.milliseconds) // Simulate work

            return GreetingResult(greeting, System.currentTimeMillis())
        }
    }

    // Register task
    app.registerTask(greetingTask)

    // Start app
    app.start()
    println("App started\n")

    // Send tasks
    println("Sending tasks...")
    val task1 = app.sendTask(
        taskName = "greet",
        kwargs = mapOf("name" to JsonPrimitive("Alice")),
        priority = 1
    )

    val task2 = app.sendTask(
        taskName = "greet",
        kwargs = mapOf("name" to JsonPrimitive("Bob")),
        queue = "default"
    )

    val task3 = app.sendTask(
        taskName = "greet",
        kwargs = mapOf("name" to JsonPrimitive("Charlie")),
        delay = 2.seconds
    )

    println("Tasks sent: ${task1.id}, ${task2.id}, ${task3.id}\n")

    // Wait for results
    println("Waiting for results...")
    val result1 = app.waitForResult(task1.id, timeout = 5.seconds)
    val result2 = app.waitForResult(task2.id, timeout = 5.seconds)
    val result3 = app.waitForResult(task3.id, timeout = 10.seconds)

    println("\nResults:")
    println("Task 1: ${result1?.status} - ${result1?.result}")
    println("Task 2: ${result2?.status} - ${result2?.result}")
    println("Task 3: ${result3?.status} - ${result3?.result}")

    // Get stats
    val stats = app.getStats()
    println("\nApp Stats:")
    println("  Running: ${stats.isRunning}")
    println("  Leader: ${stats.isLeader}")
    println("  Tasks Sent: ${stats.tasksSent}")
    println("  Workers: ${stats.workerStats?.workerCount}")

    // Shutdown
    delay(1.seconds)
    app.stop()
    ExampleConfig.shutdown()
    println("\nExample completed!")
}