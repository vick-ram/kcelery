package io.celery.examples

import io.celery.examples.config.ExampleConfig
import io.celery.task.CeleryTask
import io.celery.task.TaskConfig
import io.celery.task.TaskContext
import io.celery.scheduler.MisfirePolicy
import kotlinx.coroutines.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.serializer
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.minutes

/**
 * Email service example showing:
 * - Multiple task types
 * - Retry logic
 * - Scheduled tasks
 * - Priority queues
 * - Error handling
 */
suspend fun main() {
    println("=" .repeat(60))
    println("kCelery Email Service Example")
    println("=" .repeat(60))

    val app = ExampleConfig.createApp("email-service")

    // Email result
    @Serializable
    data class EmailResult(
        val success: Boolean,
        val messageId: String,
        val provider: String,
        val attempts: Int
    )

    // 1. Welcome Email Task
    val welcomeEmailTask = object : CeleryTask<EmailResult>(
        name = "send-welcome-email",
        maxRetries = 3,
        defaultRetryDelay = 60,
        serializer = EmailResult.serializer()
    ) {
        private var attemptCounter = 0

        override suspend fun run(context: TaskContext): EmailResult {
            val to = context.requireString("to")
            val name = context.getString("name") ?: "User"

            println("[WELCOME] Sending welcome email to: $to")

            // Simulate email sending
            delay(200.milliseconds)

            // Simulate occasional failure for demo
            attemptCounter++
            if (attemptCounter <= 1 && context.attempt < 2) {
                throw RuntimeException("SMTP server temporarily unavailable")
            }

            return EmailResult(
                success = true,
                messageId = "welcome-${context.taskId.take(8)}",
                provider = "sendgrid",
                attempts = context.attempt + 1
            )
        }

        override fun onRetry(exception: Exception, retries: Int): Long {
            println("[WELCOME] Retry $retries in ${30 * retries}s")
            return 30 * retries.toLong()
        }
    }

    // 2. Notification Email Task (lower priority)
    val notificationEmailTask = object : CeleryTask<EmailResult>(
        name = "send-notification-email",
        maxRetries = 1,
        defaultRetryDelay = 30,
        serializer = EmailResult.serializer()
    ) {
        override suspend fun run(context: TaskContext): EmailResult {
            val to = context.requireString("to")
            val subject = context.getString("subject") ?: "Notification"

            println("[NOTIFICATION] Sending '$subject' to: $to")
            delay(100)

            return EmailResult(
                success = true,
                messageId = "notif-${context.taskId.take(8)}",
                provider = "ses",
                attempts = 1
            )
        }
    }

    // 3. Bulk Email Task
    @Serializable
    data class BulkEmailResult(
        val totalSent: Int,
        val failed: Int,
        val batchId: String
    )

    val bulkEmailTask = object : CeleryTask<BulkEmailResult>(
        name = "send-bulk-email",
        maxRetries = 1,
        defaultRetryDelay = 120,
        serializer = BulkEmailResult.serializer()
    ) {
        override suspend fun run(context: TaskContext): BulkEmailResult {
            val template = context.requireString("template")
            val recipients = context.getValue<List<String>>("recipients") ?: emptyList()

            println("[BULK] Sending '$template' to ${recipients.size} recipients")

            var sent = 0
            var failed = 0

            recipients.forEach { recipient ->
                try {
                    // Simulate sending
                    delay(50)
                    sent++
                } catch (e: Exception) {
                    failed++
                }
            }

            return BulkEmailResult(sent, failed, "batch-${context.taskId.take(8)}")
        }
    }

    // Register all tasks
    app.registerTasks(welcomeEmailTask, notificationEmailTask, bulkEmailTask)

    // Configure multiple queues
    app.start()
    println("Email service started\n")

    // Send tasks to different queues
    println("1. Sending welcome emails (high priority)...")
    repeat(3) { i ->
        app.sendTask(
            taskName = "send-welcome-email",
            kwargs = mapOf(
                "to" to JsonPrimitive("user${i + 1}@example.com"),
                "name" to JsonPrimitive("User ${i + 1}")
            ),
            queue = "email",
            priority = 1
        )
    }

    println("\n2. Sending notification emails (normal priority)...")
    app.sendTask(
        taskName = "send-notification-email",
        kwargs = mapOf(
            "to" to JsonPrimitive("admin@example.com"),
            "subject" to JsonPrimitive("System Update Complete"),
            "body" to JsonPrimitive("All systems have been updated successfully.")
        ),
        queue = "email",
        priority = 5
    )

    println("\n3. Scheduling daily digest (cron)...")
    app.scheduleCron(
        scheduleId = "daily-digest",
        taskName = "send-bulk-email",
        cronExpression = "0 0 9 * * *", // 9 AM daily
        config = TaskConfig(
            maxRetries = 2,
            misfirePolicy = MisfirePolicy.FIRE_ONCE
        )
    )

    println("\n4. Scheduling weekly report (fixed delay)...")
    app.scheduleFixedDelay(
        scheduleId = "weekly-report",
        taskName = "send-notification-email",
        delay = 7.minutes * 24 * 60, // 1 week (shortened for demo)
        config = TaskConfig(maxRetries = 1)
    )

    println("\n5. Sending bulk newsletter...")
    val bulkResult = app.sendTask(
        taskName = "send-bulk-email",
        kwargs = mapOf(
            "template" to JsonPrimitive("monthly-newsletter"),
            "recipients" to JsonPrimitive(
                kotlinx.serialization.json.Json.encodeToString(
                    kotlinx.serialization.builtins.ListSerializer(serializer<String>()),
                    listOf("user1@example.com", "user2@example.com", "user3@example.com")
                )
            )
        ),
        queue = "email",
        priority = 3
    )

    // Wait for results
    delay(5.seconds)

    val bulkResultData = app.getResult(bulkResult.id)
    println("\nBulk email result: ${bulkResultData?.result}")

    // Show stats
    val stats = app.getStats()
    println("\n📊 Email Service Stats:")
    println("  Tasks Sent: ${stats.tasksSent}")
    println("  Tasks Scheduled: ${stats.tasksScheduled}")
    println("  Active Workers: ${stats.workerStats?.workerCount}")
    println("  Active Tasks: ${stats.workerStats?.activeTasks}")

    // Shutdown
    delay(2.seconds)
    app.stop()
    ExampleConfig.shutdown()
    println("\n✅ Email service example completed!")
}