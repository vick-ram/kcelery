package io.celery

import io.celery.config.InstantSerializer
import kotlinx.coroutines.delay
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive
import java.time.Instant
import kotlin.time.Duration.Companion.milliseconds

// ExampleApplication.kt - Complete example
suspend fun main() {
    // Create application
    val app = CeleryApp(
        name = "my-app",
        redisUrl = "redis://localhost:6379",
        workerThreads = 4
    )

    // Define a unified task
    val emailTask = object : CeleryTask<EmailResult>(
        name = "send-email",
        maxRetries = 3,
        defaultRetryDelay = 60,
        serializer = EmailResult.serializer()
    ) {
        override suspend fun run(context: TaskContext): EmailResult {
            val to = context.kwargs["to"]?.jsonPrimitive?.content ?: throw IllegalArgumentException("Missing 'to'")
            val subject = context.kwargs["subject"]?.jsonPrimitive?.content ?: "No Subject"
            val body = context.kwargs["body"]?.jsonPrimitive?.content ?: ""

            // Email sending logic
            println("[${context.executionTime}] Sending email to: $to")

            // Simulate work
            delay(1000.milliseconds)

            return EmailResult(
                success = true,
                messageId = "msg-${context.taskId}",
                sentAt = context.executionTime
            )
        }

        override fun onRetry(exc: Exception, retries: Int): Long {
            // Exponential backoff: 60s, 120s, 240s
            return 60 * (1L shl (retries - 1))
        }

        override fun onFailure(exc: Exception) {
            // Send alert to monitoring system
            println("ALERT: Email task failed permanently: ${exc.message}")
        }
    }

    // Register task
    app.registerTask(emailTask)

    // Start application
    app.start(workerCount = 2, workerConcurrency = 4)

    // Schedule recurring task - every 5 minutes
    app.scheduleCron(
        id = "periodic-report",
        taskName = "send-email",
        cronExpression = "0 */5 * * * *", // Every 5 minutes
        kwargs = mapOf(
            "to" to JsonPrimitive("admin@example.com"),
            "subject" to JsonPrimitive("Periodic Report"),
            "body" to JsonPrimitive("Here's your 5-minute report...")
        )
    )

    // Send async task
    val result = app.sendTask(
        taskName = "send-email",
        kwargs = mapOf(
            "to" to JsonPrimitive("user@example.com"),
            "subject" to JsonPrimitive("Welcome!"),
            "body" to JsonPrimitive("Welcome to our service!")
        ),
        priority = 1
    )

    // Check result
    delay(5000.milliseconds)
    val taskResult = app.getResult(result.getOrThrow().id)
    println("Task result: $taskResult")

    // Keep running
    delay(3600_000.milliseconds) // 1 hour

    // Shutdown
    app.shutdown()
}

@kotlinx.serialization.Serializable
data class EmailResult(
    val success: Boolean,
    val messageId: String,
    @Serializable(with = InstantSerializer::class)
    val sentAt: Instant
)