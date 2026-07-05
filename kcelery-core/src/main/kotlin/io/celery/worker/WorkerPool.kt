// kcelery-core/src/main/kotlin/io/celery/worker/WorkerPool.kt
package io.celery.worker

import io.celery.broker.MessageBroker
import io.celery.backend.ResultBackend
import io.celery.task.CeleryTask
import kotlinx.coroutines.*
import kotlinx.coroutines.time.withTimeout
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Manages a pool of workers for horizontal scaling.
 * Handles worker lifecycle, load balancing, and health monitoring.
 */
class WorkerPool(
    /** Pool configuration */
    private val config: WorkerPoolConfig = WorkerPoolConfig(),

    /** Message broker */
    private val broker: MessageBroker,

    /** Result backend */
    private val backend: ResultBackend? = null,

    /** Task registry */
    private val taskRegistry: Map<String, CeleryTask<*>> = emptyMap(),

    /** JSON serializer */
    private val json: Json = Json { ignoreUnknownKeys = true }
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    private val workers = ConcurrentHashMap<String, Worker>()
    private val isRunning = AtomicBoolean(false)
    private val workerCounter = AtomicInteger(0)

    // Health monitoring
    private var healthCheckJob: Job? = null
    private var autoScalerJob: Job? = null

    // Metrics
    private val tasksProcessed = AtomicInteger(0)
    private val tasksFailed = AtomicInteger(0)

    /**
     * Start the worker pool.
     */
    suspend fun start() {
        if (!isRunning.compareAndSet(false, true)) {
            logger.warn("Worker pool already running")
            return
        }

        logger.info("Starting worker pool with ${config.minWorkers} to ${config.maxWorkers} workers")

        // Start initial workers
        repeat(config.minWorkers) {
            startWorker()
        }

        // Start health checks
        if (config.healthCheckEnabled) {
            startHealthChecks()
        }

        // Start auto-scaler
        if (config.autoScaleEnabled) {
            startAutoScaler()
        }

        // Start metrics collection
        startMetricsCollection()

        logger.info("Worker pool started with ${workers.size} workers")
    }

    /**
     * Stop the worker pool gracefully.
     */
    suspend fun stop(timeout: Duration = 60.seconds) {
        logger.info("Stopping worker pool")
        isRunning.set(false)

        // Stop background jobs
        healthCheckJob?.cancel()
        autoScalerJob?.cancel()

        // Stop all workers
        val stopJobs = workers.values.map { worker ->
            scope.launch {
                worker.stop(timeout)
            }
        }

        // Wait for all workers to stop
        stopJobs.joinAll()
        workers.clear()
        scope.cancel()

        logger.info("Worker pool stopped. Total processed: ${tasksProcessed.get()}, " +
                "failed: ${tasksFailed.get()}")
    }

    /**
     * Scale the pool to a specific number of workers.
     */
    suspend fun scaleTo(targetCount: Int) {
        val currentCount = workers.size
        val adjustedTarget = targetCount.coerceIn(config.minWorkers, config.maxWorkers)

        when {
            adjustedTarget > currentCount -> {
                val toAdd = adjustedTarget - currentCount
                logger.info("Scaling up: adding $toAdd workers")
                repeat(toAdd) { startWorker() }
            }
            adjustedTarget < currentCount -> {
                val toRemove = currentCount - adjustedTarget
                logger.info("Scaling down: removing $toRemove workers")
                repeat(toRemove) { stopWorker() }
            }
        }
    }

    /**
     * Get pool statistics.
     */
    fun getStats(): PoolStats {
        val workerStats = workers.values.map { it.getStats() }
        return PoolStats(
            workerCount = workers.size,
            minWorkers = config.minWorkers,
            maxWorkers = config.maxWorkers,
            activeTasks = workerStats.sumOf { it.activeTasks },
            processedTasks = tasksProcessed.get(),
            failedTasks = tasksFailed.get(),
            workers = workerStats,
            queueSizes = config.queues.associateWith { queue ->
                runBlocking { broker.queueSize(queue) }
            }
        )
    }

    /**
     * Get a specific worker by name.
     */
    fun getWorker(name: String): Worker? = workers[name]

    /**
     * Revoke a task across all workers.
     */
    fun revokeTask(taskId: String): Boolean {
        return workers.values.any { it.revokeTask(taskId) }
    }

    private fun startWorker(): Worker {
        val workerId = workerCounter.incrementAndGet()
        val workerName = "${config.workerNamePrefix}-$workerId"

        val worker = Worker(
            name = workerName,
            queues = config.queues,
            concurrency = config.workerConcurrency,
            broker = broker,
            backend = backend,
            taskRegistry = taskRegistry,
            json = json,
            consumerGroup = config.consumerGroup
        )

        workers[workerName] = worker
        scope.launch {
            try {
                worker.start()
            } catch (e: Exception) {
                logger.error("Worker $workerName failed to start", e)
                workers.remove(workerName)
                // Attempt to restart if pool is still running
                if (isRunning.get() && workers.size < config.minWorkers) {
                    startWorker()
                }
            }
        }

        logger.info("Started worker: $workerName")
        return worker
    }

    private suspend fun stopWorker() {
        val worker = workers.entries.firstOrNull()?.value ?: return

        worker.stop(config.workerShutdownTimeout)
        workers.remove(worker.name)
        logger.info("Stopped worker: ${worker.name}")
    }

    private fun startHealthChecks() {
        healthCheckJob = scope.launch {
            while (isActive && isRunning.get()) {
                try {
                    performHealthCheck()
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.error("Health check failed", e)
                }
                delay(config.healthCheckInterval)
            }
        }
    }

    private suspend fun performHealthCheck() {
        val unhealthyWorkers = mutableListOf<String>()

        workers.forEach { (name, worker) ->
            try {
                withTimeout(5.seconds) {
                    // Check if worker is responsive
                    val stats = worker.getStats()
                    if (stats.processedTasks == 0 && worker.isActive) {
                        // Worker hasn't processed anything, check if stuck
                        logger.warn("Worker $name may be unhealthy: no tasks processed")
                    }
                }
            } catch (e: TimeoutCancellationException) {
                logger.error("Worker $name health check timed out")
                unhealthyWorkers.add(name)
            } catch (e: Exception) {
                logger.error("Worker $name health check failed", e)
                unhealthyWorkers.add(name)
            }
        }

        // Restart unhealthy workers
        unhealthyWorkers.forEach { workerName ->
            logger.warn("Restarting unhealthy worker: $workerName")
            workers.remove(workerName)
            if (workers.size < config.minWorkers) {
                startWorker()
            }
        }
    }

    private fun startAutoScaler() {
        autoScalerJob = scope.launch {
            while (isActive && isRunning.get()) {
                try {
                    performAutoScale()
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.error("Auto-scale check failed", e)
                }
                delay(config.autoScaleInterval)
            }
        }
    }

    private suspend fun performAutoScale() {
        val currentWorkers = workers.size

        // Calculate total queue size
        val totalQueueSize = config.queues.sumOf { queue ->
            try {
                broker.queueSize(queue)
            } catch (e: Exception) {
                logger.error("Failed to get queue size for $queue", e)
                0L
            }
        }

        // Scale up if queue is growing
        if (totalQueueSize > config.scaleUpThreshold && currentWorkers < config.maxWorkers) {
            val toAdd = minOf(
                config.scaleUpIncrement.toInt(),
                config.maxWorkers - currentWorkers
            )
            logger.info("Auto-scaling UP: adding $toAdd workers (queue size: $totalQueueSize)")
            repeat(toAdd) { startWorker() }
        }

        // Scale down if queue is small
        if (totalQueueSize < config.scaleDownThreshold && currentWorkers > config.minWorkers) {
            val toRemove = minOf(
                config.scaleDownIncrement.toInt(),
                currentWorkers - config.minWorkers
            )
            logger.info("Auto-scaling DOWN: removing $toRemove workers (queue size: $totalQueueSize)")
            repeat(toRemove) { stopWorker() }
        }
    }

    private fun startMetricsCollection() {
        scope.launch {
            while (isActive && isRunning.get()) {
                try {
                    val stats = getStats()
                    logger.debug("Pool metrics: ${stats.workerCount} workers, " +
                            "${stats.activeTasks} active, ${stats.processedTasks} processed")

                    // Update counters
                    workers.values.forEach { worker ->
                        val ws = worker.getStats()
                        tasksProcessed.addAndGet(ws.processedTasks)
                        tasksFailed.addAndGet(ws.failedTasks)
                    }
                } catch (e: Exception) {
                    logger.error("Metrics collection failed", e)
                }
                delay(config.metricsInterval)
            }
        }
    }
}

/**
 * Worker pool configuration.
 */
data class WorkerPoolConfig(
    /** Queue names to consume from */
    val queues: List<String> = listOf("default"),

    /** Minimum number of workers */
    val minWorkers: Int = 2,

    /** Maximum number of workers */
    val maxWorkers: Int = 10,

    /** Concurrency per worker */
    val workerConcurrency: Int = 4,

    /** Worker name prefix */
    val workerNamePrefix: String = "celery-worker",

    /** Consumer group name */
    val consumerGroup: String = "celery-workers",

    /** Worker shutdown timeout */
    val workerShutdownTimeout: Duration = 30.seconds,

    /** Enable health checks */
    val healthCheckEnabled: Boolean = true,

    /** Health check interval */
    val healthCheckInterval: Duration = 30.seconds,

    /** Enable auto-scaling */
    val autoScaleEnabled: Boolean = true,

    /** Auto-scale check interval */
    val autoScaleInterval: Duration = 10.seconds,

    /** Queue size threshold to scale up */
    val scaleUpThreshold: Long = 100,

    /** Queue size threshold to scale down */
    val scaleDownThreshold: Long = 10,

    /** Number of workers to add when scaling up */
    val scaleUpIncrement: Long = 2,

    /** Number of workers to remove when scaling down */
    val scaleDownIncrement: Long = 1,

    /** Metrics collection interval */
    val metricsInterval: Duration = 60.seconds
)

/**
 * Pool statistics.
 */
data class PoolStats(
    val workerCount: Int,
    val minWorkers: Int,
    val maxWorkers: Int,
    val activeTasks: Int,
    val processedTasks: Int,
    val failedTasks: Int,
    val workers: List<WorkerStats>,
    val queueSizes: Map<String, Long>
)