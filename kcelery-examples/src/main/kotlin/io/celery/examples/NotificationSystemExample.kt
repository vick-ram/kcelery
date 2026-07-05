package io.celery.examples

import io.celery.examples.config.ExampleConfig
import io.celery.task.CeleryTask
import io.celery.task.TaskContext
import kotlinx.coroutines.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonPrimitive
import kotlin.time.Duration.Companion.seconds
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger


// Notification channels
enum class Channel { PUSH, EMAIL, SMS, IN_APP }


/**
 * Notification system example showing:
 * - Multiple channels (push, email, SMS, in-app)
 * - User preferences
 * - Channel-specific retry logic
 * - Rate limiting simulation
 * - Batching
 */

suspend fun main() {
    println("=" .repeat(60))
    println("kCelery Notification System Example")
    println("=" .repeat(60))

    val app = ExampleConfig.createApp("notification-system")

    @Serializable
    data class NotificationResult(
        val channel: String,
        val delivered: Boolean,
        val provider: String,
        val latency: Long
    )

    // Track delivery stats
    val deliveryStats = ConcurrentHashMap<Channel, AtomicInteger>()
    Channel.entries.forEach { deliveryStats[it] = AtomicInteger(0) }

    // 1. Push Notification Task
    val pushTask = object : CeleryTask<NotificationResult>(
        name = "send-push-notification",
        maxRetries = 3,
        defaultRetryDelay = 10,
        serializer = NotificationResult.serializer()
    ) {
        override suspend fun run(context: TaskContext): NotificationResult {
            val userId = context.requireString("userId")
            val title = context.getString("title") ?: "Notification"
            val body = context.getString("body") ?: ""

            println("📱 [PUSH] Sending to user $userId: $title")

            // Simulate FCM call
            delay(100)

            // Simulate 90% success rate
            val success = Math.random() > 0.1
            if (!success && context.attempt < 2) {
                throw RuntimeException("FCM service unavailable")
            }

            deliveryStats[Channel.PUSH]?.incrementAndGet()

            return NotificationResult(
                channel = "PUSH",
                delivered = success,
                provider = "firebase",
                latency = (50..200).random().toLong()
            )
        }
    }

    // 2. Email Notification Task
    val emailTask = object : CeleryTask<NotificationResult>(
        name = "send-email-notification",
        maxRetries = 2,
        defaultRetryDelay = 30,
        serializer = NotificationResult.serializer()
    ) {
        override suspend fun run(context: TaskContext): NotificationResult {
            val to = context.requireString("to")
            val subject = context.getString("subject") ?: "Notification"

            println("📧 [EMAIL] Sending to $to: $subject")
            delay(150)

            deliveryStats[Channel.EMAIL]?.incrementAndGet()

            return NotificationResult(
                channel = "EMAIL",
                delivered = true,
                provider = "sendgrid",
                latency = (100..500).random().toLong()
            )
        }
    }

    // 3. SMS Notification Task
    val smsTask = object : CeleryTask<NotificationResult>(
        name = "send-sms-notification",
        maxRetries = 2,
        defaultRetryDelay = 15,
        serializer = NotificationResult.serializer()
    ) {
        override suspend fun run(context: TaskContext): NotificationResult {
            val phone = context.requireString("phone")
            val message = context.getString("message") ?: ""

            println("📲 [SMS] Sending to $phone: ${message.take(30)}...")
            delay(200)

            // Simulate carrier issues
            val success = Math.random() > 0.05

            if (success) {
                deliveryStats[Channel.SMS]?.incrementAndGet()
            }

            return NotificationResult(
                channel = "SMS",
                delivered = success,
                provider = "twilio",
                latency = (200..1000).random().toLong()
            )
        }
    }

    // 4. In-App Notification Task
    val inAppTask = object : CeleryTask<NotificationResult>(
        name = "send-inapp-notification",
        maxRetries = 1,
        defaultRetryDelay = 5,
        serializer = NotificationResult.serializer()
    ) {
        override suspend fun run(context: TaskContext): NotificationResult {
            val userId = context.requireString("userId")
            val type = context.getString("type") ?: "info"

            println("🔔 [IN-APP] Notification for user $userId: $type")
            delay(50)

            deliveryStats[Channel.IN_APP]?.incrementAndGet()

            return NotificationResult(
                channel = "IN_APP",
                delivered = true,
                provider = "websocket",
                latency = (10..50).random().toLong()
            )
        }
    }

    // Register tasks
    app.registerTasks(pushTask, emailTask, smsTask, inAppTask)
    app.start()
    println("Notification system started\n")

    // Simulate user signup - send welcome notifications via all channels
    println("1. User signup - sending welcome notifications...")
    val userId = "user-${System.currentTimeMillis()}"

    // Push
    app.sendTask(
        taskName = "send-push-notification",
        kwargs = mapOf(
            "userId" to JsonPrimitive(userId),
            "title" to JsonPrimitive("Welcome! 🎉"),
            "body" to JsonPrimitive("Thanks for joining our platform!")
        ),
        queue = "notifications",
        priority = 1
    )

    // Email
    app.sendTask(
        taskName = "send-email-notification",
        kwargs = mapOf(
            "to" to JsonPrimitive("$userId@example.com"),
            "subject" to JsonPrimitive("Welcome to Our Platform!"),
            "body" to JsonPrimitive("We're excited to have you on board.")
        ),
        queue = "notifications",
        priority = 2
    )

    // In-app
    app.sendTask(
        taskName = "send-inapp-notification",
        kwargs = mapOf(
            "userId" to JsonPrimitive(userId),
            "type" to JsonPrimitive("welcome"),
            "message" to JsonPrimitive("Welcome! Complete your profile to get started.")
        ),
        queue = "notifications",
        priority = 1
    )

    // Simulate order confirmation
    println("\n2. Order confirmation - high priority notifications...")
    delay(1.seconds)

    val orderId = "ORD-${System.currentTimeMillis()}"

    // Push (high priority)
    app.sendTask(
        taskName = "send-push-notification",
        kwargs = mapOf(
            "userId" to JsonPrimitive(userId),
            "title" to JsonPrimitive("Order Confirmed! ✅"),
            "body" to JsonPrimitive("Your order #$orderId has been confirmed.")
        ),
        queue = "notifications",
        priority = 0 // Highest priority
    )

    // Email (order receipt)
    app.sendTask(
        taskName = "send-email-notification",
        kwargs = mapOf(
            "to" to JsonPrimitive("$userId@example.com"),
            "subject" to JsonPrimitive("Order Confirmation #$orderId"),
            "body" to JsonPrimitive("Your order has been confirmed and is being processed.")
        ),
        queue = "notifications",
        priority = 1
    )

    // SMS for high-value orders
    if (Math.random() > 0.5) {
        app.sendTask(
            taskName = "send-sms-notification",
            kwargs = mapOf(
                "phone" to JsonPrimitive("+1234567890"),
                "message" to JsonPrimitive("Order #$orderId confirmed! Track: example.com/track/$orderId")
            ),
            queue = "notifications",
            priority = 0
        )
    }

    // Simulate batch notification
    println("\n3. Sending batch promotion notifications...")
    delay(1.seconds)

    val users = (1..10).map { "user-$it" }
    users.forEach { uid ->
        app.sendTask(
            taskName = "send-push-notification",
            kwargs = mapOf(
                "userId" to JsonPrimitive(uid),
                "title" to JsonPrimitive("Flash Sale! 🔥"),
                "body" to JsonPrimitive("50% off for the next 2 hours!")
            ),
            queue = "notifications",
            priority = 5 // Low priority for promotions
        )
    }

    // Wait for processing
    println("\n⏳ Processing notifications...")
    delay(5.seconds)

    // Show delivery stats
    println("\n📊 Delivery Statistics:")
    deliveryStats.forEach { (channel, count) ->
        println("  ${channel.name.padEnd(8)}: ${count.get()} delivered")
    }

    // App stats
    val stats = app.getStats()
    println("\n📊 System Stats:")
    println("  Total Tasks Sent: ${stats.tasksSent}")
    println("  Active Workers: ${stats.workerStats?.workerCount}")
    println("  Processed: ${stats.workerStats?.processedTasks}")
    println("  Failed: ${stats.workerStats?.failedTasks}")

    // Shutdown
    app.stop()
    ExampleConfig.shutdown()
    println("\n✅ Notification system example completed!")
}