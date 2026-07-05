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
 * Batch processing example showing:
 * - Large-scale task distribution
 * - Result aggregation
 * - Progress tracking
 * - Error handling in batches
 */
suspend fun main() {
    println("=" .repeat(60))
    println("kCelery Batch Processing Example")
    println("=" .repeat(60))

    val app = ExampleConfig.createApp("batch-processor")

    // 1. Data Processing Task
    @Serializable
    data class ProcessResult(
        val batchId: String,
        val processed: Int,
        val errors: Int,
        val duration: Long
    )

    val processTask = object : CeleryTask<ProcessResult>(
        name = "process-item",
        maxRetries = 2,
        defaultRetryDelay = 5,
        serializer = ProcessResult.serializer()
    ) {
        override suspend fun run(context: TaskContext): ProcessResult {
            val itemId = context.requireString("itemId")
            val batchId = context.getString("batchId") ?: "unknown"

            // Simulate processing
            delay((10..100).random().toLong().milliseconds)

            // Simulate occasional failures
            if (Math.random() < 0.1) {
                throw RuntimeException("Failed to process item: $itemId")
            }

            return ProcessResult(batchId, 1, 0, System.currentTimeMillis())
        }
    }

    // 2. Aggregation Task
    @Serializable
    data class AggregateResult(
        val totalItems: Int,
        val totalProcessed: Int,
        val totalErrors: Int,
        val batchId: String
    )

    val aggregateTask = object : CeleryTask<AggregateResult>(
        name = "aggregate-results",
        maxRetries = 1,
        defaultRetryDelay = 10,
        serializer = AggregateResult.serializer()
    ) {
        override suspend fun run(context: TaskContext): AggregateResult {
            val batchId = context.requireString("batchId")
            val itemCount = context.getValue<Int>("itemCount") ?: 0

            println("[AGGREGATE] Aggregating results for batch: $batchId ($itemCount items)")

            // Wait for all items to be processed
            delay(2.seconds)

            // In real scenario, you'd query the backend for all results
            val processed = (itemCount * 0.9).toInt() // 90% success rate
            val errors = itemCount - processed

            return AggregateResult(itemCount, processed, errors, batchId)
        }
    }

    app.registerTasks(processTask, aggregateTask)
    app.start()
    println("Batch processor started\n")

    // Process a batch of 100 items
    val batchId = "batch-${System.currentTimeMillis()}"
    val totalItems = 100

    println("Starting batch processing...")
    println("Batch ID: $batchId")
    println("Total items: $totalItems\n")

    val startTime = System.currentTimeMillis()

    // Send all items for processing
    val tasks = (1..totalItems).map { itemNumber ->
        app.sendTask(
            taskName = "process-item",
            kwargs = mapOf(
                "itemId" to JsonPrimitive("item-$itemNumber"),
                "batchId" to JsonPrimitive(batchId)
            ),
            queue = "processing",
            priority = (itemNumber % 3) // Distribute priority
        )
    }

    println("✅ ${tasks.size} tasks sent for processing\n")

    // Send aggregation task
    app.sendTask(
        taskName = "aggregate-results",
        kwargs = mapOf(
            "batchId" to JsonPrimitive(batchId),
            "itemCount" to JsonPrimitive(totalItems.toString())
        ),
        queue = "processing",
        priority = 0,
        delay = 3.seconds // Start aggregation after processing
    )

    // Monitor progress
    println("Monitoring progress...")
    var completed = 0
    var attempts = 0

    while (completed < totalItems && attempts < 30) {
        delay(1.seconds)
        attempts++

        // Check completed count (simplified - in real scenario query backend)
        val stats = app.getStats()
        completed = stats.workerStats?.processedTasks ?: 0

        val progress = (completed.toDouble() / totalItems * 100).toInt()
        val bar = "█".repeat(progress / 2) + "░".repeat(50 - progress / 2)
        println("\r   [$bar] $progress% ($completed/$totalItems)")
    }

    val elapsed = System.currentTimeMillis() - startTime
    println("\n\n✅ Batch processing completed in ${elapsed}ms")

    // Final stats
    val finalStats = app.getStats()
    println("\n📊 Processing Stats:")
    println("  Total Sent: ${finalStats.tasksSent}")
    println("  Processed: ${finalStats.workerStats?.processedTasks}")
    println("  Failed: ${finalStats.workerStats?.failedTasks}")
    println("  Time: ${elapsed}ms")
    println("  Throughput: ${totalItems * 1000 / maxOf(elapsed, 1)} items/sec")

    app.stop()
    ExampleConfig.shutdown()
    println("\n✅ Batch processing example completed!")
}