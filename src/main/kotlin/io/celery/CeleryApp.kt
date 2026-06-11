package io.celery

import io.celery.core.Clock
import io.celery.core.SerializerRegistry
import io.celery.redis.RedisDistributedLockManager
import io.celery.metrics.SchedulerMetrics
import io.celery.model.CeleryTask
import io.celery.model.TaskMessage
import io.celery.model.TaskRegistry
import io.celery.model.TaskResult
import io.celery.redis.MessageBroker
import io.celery.redis.RedisBackend
import io.celery.redis.RedisBroker
import io.celery.redis.ResultBackend
import io.celery.scheduler.CelerySchedulerBridge
import io.celery.worker.Worker
import io.lettuce.core.ClientOptions
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.MaintNotificationsConfig
import io.lettuce.core.RedisClient
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.coroutines.*
import kotlinx.coroutines.time.withTimeout
import kotlinx.serialization.json.Json
import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.config.Config
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import kotlin.time.Duration.Companion.milliseconds


@OptIn(ExperimentalLettuceCoroutinesApi::class)
class CeleryApp(
    private val name: String = "celery-app",
    private val redisUrl: String = "redis://localhost:6379",
    private val workerThreads: Int = Runtime.getRuntime().availableProcessors()
) {
    private val logger = LoggerFactory.getLogger(CeleryApp::class.java)

    private val json = Json {
        ignoreUnknownKeys = true
        prettyPrint = false
        isLenient = true
    }
    private val clock = Clock.utc()

    private val redisClient = RedisClient.create(redisUrl)
    private val redisCommands: RedisCoroutinesCommands<String, String>
    private val redissonClient: RedissonClient

    // Core components
    private val broker: MessageBroker
    private val backend: ResultBackend
    private val lockManager: RedisDistributedLockManager
    private val metrics: SchedulerMetrics
    private val bridge: CelerySchedulerBridge
    private val serializerRegistry: SerializerRegistry // Declare SerializerRegistry here
    private val taskRegistry: TaskRegistry // Declare TaskRegistry here

    // Workers
    private val workers = mutableListOf<Worker>()
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    init {
        // Initialize Redis connections
        val clientOptions = ClientOptions.builder()
            .maintNotificationsConfig(MaintNotificationsConfig.disabled())
            .build()

        redisClient.options = clientOptions

        redisCommands = redisClient.connect().coroutines()

        val redissonConfig = Config().apply {
            useSingleServer().address = redisUrl
        }
        redissonClient = Redisson.create(redissonConfig)

        // Initialize components
        broker = RedisBroker(redisCommands, json, name)
        backend = RedisBackend(redisCommands, json, name)
        lockManager = RedisDistributedLockManager(redissonClient)

        val meterRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        metrics = SchedulerMetrics(meterRegistry, name)

        serializerRegistry = SerializerRegistry() // Initialize SerializerRegistry
        taskRegistry = TaskRegistry() // Initialize TaskRegistry

        // Initialize bridge
        bridge = CelerySchedulerBridge(
            clock = clock,
            json = json,
            redis = redisCommands,
            lockManager = lockManager,
            metrics = metrics,
            broker = broker,
            backend = backend
        )
    }

    /**
     * Register a task that can be both scheduled and dispatched async
     */
    fun registerTask(task: CeleryTask<*>) {
        taskRegistry.register(task) // Register task with the taskRegistry
        bridge.registerTask(task)
        serializerRegistry.register(task.serializer::class, task.serializer) // Register serializer for the task
        logger.info("Registered unified task: ${task.name}")
    }

    /**
     * Schedule a task using cron expression
     */
    suspend fun scheduleCron(
        id: String,
        taskName: String,
        cronExpression: String,
        args: List<kotlinx.serialization.json.JsonElement> = emptyList(),
        kwargs: Map<String, kotlinx.serialization.json.JsonElement> = emptyMap()
    ): Result<TaskResult> {
        return bridge.scheduleCron(id, taskName, cronExpression, args, kwargs)
    }

    /**
     * Send async task
     */
    suspend fun sendTask(
        taskName: String,
        args: List<kotlinx.serialization.json.JsonElement> = emptyList(),
        kwargs: Map<String, kotlinx.serialization.json.JsonElement> = emptyMap(),
        queue: String = "default",
        priority: Int = 0,
        countdown: Long? = null,
        eta: Instant? = null
    ): Result<TaskMessage> {
        return bridge.sendTask(taskName, args, kwargs, queue, priority, countdown, eta)
    }

    /**
     * Start the application with all components
     */
    suspend fun start(workerCount: Int = 1, workerConcurrency: Int = 4) {
        logger.info("Starting CeleryApp: $name")

        // Start bridge (includes scheduler)
        bridge.start()

        // Start Celery workers
        // val taskRegistry = TaskRegistry() // Remove instantiation here
        // Note: In production, you'd extract tasks from bridge

        repeat(workerCount) { index ->
            val worker = Worker(
                name = "${name}-worker-${index + 1}",
                queues = listOf("default"),
                concurrency = workerConcurrency,
                broker = broker,
                backend = backend,
                taskRegistry = taskRegistry,
                serializerRegistry = serializerRegistry // Pass serializerRegistry here
            )
            workers.add(worker)
            worker.start()
        }

        // Start heartbeat monitor
        startHeartbeat()

        logger.info("Application started with $workerCount workers")
    }

    /**
     * Get task result
     */
    suspend fun getResult(taskId: String): TaskResult? {
        return bridge.getResult(taskId)
    }

    /**
     * Cancel a task
     */
    suspend fun cancelTask(taskId: String): Boolean {
        return bridge.cancelTask(taskId)
    }

    /**
     * Graceful shutdown
     */
    suspend fun shutdown(timeout: Duration = Duration.ofSeconds(30)) {
        logger.info("Shutting down UnifiedCeleryApp: $name")

        withTimeout(timeout) {
            // Stop bridge first
            bridge.stop()

            // Stop workers
            workers.forEach { it.stop() }
            workers.clear()

            // Close connections
            broker.close()
            backend.close()

            // Clean up Redis connections
            redisClient.shutdown()
            redissonClient.shutdown()

            scope.cancel()
        }

        logger.info("Application shutdown complete")
    }

    private fun startHeartbeat() {
        scope.launch {
            while (isActive) {
                try {
                    val stats = getAppStats()
                    logger.info("Heartbeat: ${stats}")
                    delay(30_000.milliseconds)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.error("Heartbeat failed", e)
                }
            }
        }
    }

    data class AppStats(
        val workers: Int,
        val activeJobs: Int,
        val scheduledTasks: Int
    )

    private fun getAppStats(): AppStats {
        return AppStats(
            workers = workers.size,
            activeJobs = 0, // TODO: Track active jobs
            scheduledTasks = 0 // TODO: Track scheduled tasks
        )
    }
}