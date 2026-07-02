package io.celery

import io.celery.backend.ResultBackend
import io.celery.backend.ResultStatus
import io.celery.backend.TaskResult
import io.celery.broker.MessageBroker
import io.celery.deadletter.DeadLetterQueue
import io.celery.lock.DistributedLock
import io.celery.lock.LeaderElector
import io.celery.lock.LockHandle
import io.celery.scheduler.CronScheduler
import io.celery.scheduler.FixedDelayScheduler
import io.celery.scheduler.FixedRateScheduler
import io.celery.task.CeleryTask
import io.celery.task.TaskConfig
import io.celery.task.TaskContext
import io.celery.task.TaskMessage
import io.celery.worker.WorkerPool
import io.celery.worker.WorkerPoolConfig
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

/**
 * Central orchestrator for kCelery.
 * Wires together broker, backend, locks, schedulers, and workers.
 *
 * This is the main entry point for applications using kCelery.
 *
 * Example usage:
 * ```kotlin
 * val app = CeleryApp.builder()
 *     .withName("my-app")
 *     .withBroker(redisBroker)
 *     .withBackend(redisBackend)
 *     .withLock(redisLock)
 *     .withLeaderElector(redisLeaderElector)
 *     .build()
 *
 * app.registerTask(myTask)
 * app.start()
 *
 * // Send tasks
 * app.sendTask("my-task", args = listOf(JsonPrimitive("arg1")))
 *
 * // Schedule tasks
 * app.scheduleCron("daily-task", "my-task", "0 0 9 * * *")
 *
 * // Shutdown
 * app.stop()
 * ```
 */
class CeleryApp private constructor(
    private val config: CeleryConfig
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    // Core components
    private val broker: MessageBroker = config.broker
        ?: throw IllegalStateException("Broker is required")
    private val backend: ResultBackend? = config.backend
    private val lock: DistributedLock? = config.lock
    private val leaderElector: LeaderElector? = config.leaderElector
    private val deadLetterQueue: DeadLetterQueue? = config.deadLetterQueue

    // Task registry
    private val taskRegistry = ConcurrentHashMap<String, CeleryTask<*>>()

    // Schedulers
    private val cronScheduler: CronScheduler?
    private val fixedDelayScheduler: FixedDelayScheduler?
    private val fixedRateScheduler: FixedRateScheduler?

    // Worker management
    private var workerPool: WorkerPool? = null

    // Application state
    private val isRunning = AtomicBoolean(false)
    private val appScope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    // Leadership
    private var leadershipHandle: LockHandle? = null
    private val isLeader = MutableStateFlow(false)

    // JSON serializer
    private val json: Json = config.json

    // Metrics
    private val tasksSent = java.util.concurrent.atomic.AtomicLong(0)
    private val tasksScheduled = java.util.concurrent.atomic.AtomicLong(0)

    init {
        // Initialize schedulers with task executor
        val taskExecutor: suspend (String, TaskContext) -> Unit = { taskName, context ->
            executeTask(taskName, context)
        }

        cronScheduler = if (config.enableCronScheduler) {
            CronScheduler(taskExecutor)
        } else null

        fixedDelayScheduler = if (config.enableFixedDelayScheduler) {
            FixedDelayScheduler(taskExecutor)
        } else null

        fixedRateScheduler = if (config.enableFixedRateScheduler) {
            FixedRateScheduler(taskExecutor)
        } else null

        logger.info("CeleryApp '${config.name}' initialized")
    }

    // ==================== Task Registration ====================

    /**
     * Register a task handler.
     *
     * @param task The task to register
     * @throws IllegalArgumentException if a task with the same name is already registered
     */
    fun registerTask(task: CeleryTask<*>) {
        require(!taskRegistry.containsKey(task.name)){
            "Task '${task.name}' is already registered"
        }
        taskRegistry[task.name] = task
        logger.info("Registered task: ${task.name}")
    }

    /**
     * Register multiple tasks.
     */
    fun registerTasks(vararg tasks: CeleryTask<*>) {
        tasks.forEach { registerTask(it) }
    }

    /**
     * Unregister a task.
     */
    fun unregisterTask(taskName: String) {
        taskRegistry.remove(taskName)
        logger.info("Unregistered task: $taskName")
    }

    /**
     * Check if a task is registered.
     */
    fun isTaskRegistered(taskName: String): Boolean {
        return taskRegistry.containsKey(taskName)
    }

    /**
     * Get all registered task names.
     */
    fun getRegisteredTasks(): Set<String> = taskRegistry.keys.toSet()

    // ==================== Async Task Sending ====================

    /**
     * Send a task for async execution.
     *
     * @param taskName Registered task name
     * @param args Task arguments
     * @param kwargs Task keyword arguments
     * @param queue Target queue name
     * @param priority Task priority (0 = highest)
     * @param delay Delay before execution
     * @param expires Expiration time
     * @param headers Additional headers
     * @return TaskMessage with the task ID
     */
    suspend fun sendTask(
        taskName: String,
        args: List<JsonElement> = emptyList(),
        kwargs: Map<String, JsonElement> = emptyMap(),
        queue: String = "default",
        priority: Int = 0,
        delay: Duration? = null,
        expires: Duration? = null,
        headers: Map<String, String> = emptyMap()
    ): TaskMessage {
        require(taskRegistry.containsKey(taskName)) {
            "Task '$taskName' is not registered. Registered tasks: ${taskRegistry.keys}"
        }

        val message = TaskMessage.create(
            taskName = taskName,
            args = args,
            kwargs = kwargs,
            priority = priority,
            queue = queue,
            delay = delay,
            expires = expires,
            headers = headers
        )

        broker.enqueue(message, queue)

        // Store initial state
        backend?.storeResult(
            message.id,
            TaskResult(
                taskId = message.id,
                status = ResultStatus.PENDING
            ),
            expiry = config.resultExpiry
        )

        tasksSent.incrementAndGet()
        logger.debug("Sent task: $taskName [${message.id}] to queue: $queue")

        return message
    }

    /**
     * Send a task with a simple string argument.
     */
    suspend fun sendTask(
        taskName: String,
        arg: String,
        queue: String = "default",
        priority: Int = 0
    ): TaskMessage {
        return sendTask(
            taskName = taskName,
            args = listOf(JsonPrimitive(arg)),
            queue = queue,
            priority = priority
        )
    }

    /**
     * Send a batch of tasks.
     */
    suspend fun sendBatch(
        taskName: String,
        batchArgs: List<List<JsonElement>>,
        queue: String = "default",
        priority: Int = 0
    ): List<TaskMessage> {
        return batchArgs.map { args ->
            sendTask(taskName, args = args, queue = queue, priority = priority)
        }
    }

    // ==================== Scheduled Tasks ====================

    /**
     * Schedule a task with a cron expression.
     *
     * @param scheduleId Unique schedule identifier
     * @param taskName Registered task name
     * @param cronExpression Cron expression (e.g., "0 0 9 * * *" for 9 AM daily)
     * @param config Task configuration
     * @return Schedule ID
     */
    fun scheduleCron(
        scheduleId: String,
        taskName: String,
        cronExpression: String,
        config: TaskConfig = TaskConfig()
    ): String {
        require(taskRegistry.containsKey(taskName)) { "Task '$taskName' is not registered" }
        require(cronScheduler != null) { "Cron scheduler is not enabled" }

        val id = cronScheduler.schedule(taskName, cronExpression, config)
        tasksScheduled.incrementAndGet()

        logger.info("Scheduled cron task: $taskName [$scheduleId] - $cronExpression")
        return id
    }

    /**
     * Schedule a task with a fixed delay.
     *
     * @param scheduleId Unique schedule identifier
     * @param taskName Registered task name
     * @param delay Delay between executions
     * @param config Task configuration
     * @return Schedule ID
     */
    fun scheduleFixedDelay(
        scheduleId: String,
        taskName: String,
        delay: Duration,
        config: TaskConfig = TaskConfig()
    ): String {
        require(taskRegistry.containsKey(taskName)) { "Task '$taskName' is not registered" }
        require(fixedDelayScheduler != null) { "Fixed delay scheduler is not enabled" }

        val id = fixedDelayScheduler.schedule(taskName, delay, config)
        tasksScheduled.incrementAndGet()

        logger.info("Scheduled fixed-delay task: $taskName [$scheduleId] - $delay")
        return id
    }

    /**
     * Schedule a task at a fixed rate.
     *
     * @param scheduleId Unique schedule identifier
     * @param taskName Registered task name
     * @param period Period between executions
     * @param startTime Optional start time
     * @param config Task configuration
     * @return Schedule ID
     */
    fun scheduleFixedRate(
        scheduleId: String,
        taskName: String,
        period: Duration,
        startTime: Instant? = null,
        config: TaskConfig = TaskConfig()
    ): String {
        require(taskRegistry.containsKey(taskName)) { "Task '$taskName' is not registered" }
        require(fixedRateScheduler != null) { "Fixed rate scheduler is not enabled" }

        val id = fixedRateScheduler.schedule(taskName, period, config, startTime)
        tasksScheduled.incrementAndGet()

        logger.info("Scheduled fixed-rate task: $taskName [$scheduleId] - $period")
        return id
    }

    /**
     * Unschedule a task.
     */
    fun unschedule(scheduleId: String, schedulerType: SchedulerType = SchedulerType.CRON) {
        when (schedulerType) {
            SchedulerType.CRON -> cronScheduler?.unschedule(scheduleId)
            SchedulerType.FIXED_DELAY -> fixedDelayScheduler?.unschedule(scheduleId)
            SchedulerType.FIXED_RATE -> fixedRateScheduler?.unschedule(scheduleId)
        }
        logger.info("Unscheduled task: $scheduleId")
    }

    // ==================== Result Management ====================

    /**
     * Get a task result.
     */
    suspend fun getResult(taskId: String): TaskResult? {
        return backend?.getResult(taskId)
    }

    /**
     * Get task status.
     */
    suspend fun getStatus(taskId: String): ResultStatus? {
        return backend?.getStatus(taskId)
    }

    /**
     * Wait for a task to complete.
     */
    suspend fun waitForResult(
        taskId: String,
        timeout: Duration = 30.seconds,
        pollInterval: Duration = 100.milliseconds
    ): TaskResult? =
        withTimeoutOrNull(timeout) {
            var result: TaskResult?

            do {
                result = backend?.getResult(taskId)
                if (result?.status?.isTerminal() == true) {
                    break
                }
                delay(pollInterval)
            } while (true)

            result
        }


    /**
     * Revoke (cancel) a task.
     */
    suspend fun revokeTask(taskId: String): Boolean {
        backend?.revokeTask(taskId)
        workerPool?.revokeTask(taskId)
        logger.info("Revoked task: $taskId")
        return true
    }

    // ==================== Application Lifecycle ====================

    /**
     * Start the application.
     * Starts schedulers, workers, and acquires leadership if configured.
     */
    suspend fun start() {
        if (!isRunning.compareAndSet(false, true)) {
            logger.warn("CeleryApp '${config.name}' is already running")
            return
        }

        logger.info("Starting CeleryApp '${config.name}'...")

        // Acquire leadership if configured
        if (leaderElector != null) {
            leadershipHandle = leaderElector.campaign(config.leaderGroup, config.leaderTtl)
            if (leadershipHandle != null) {
                isLeader.value = true
                logger.info("Acquired leadership for group: ${config.leaderGroup}")
            } else {
                logger.info("Running as follower in group: ${config.leaderGroup}")
            }
        } else {
            // No leader election, always act as leader
            isLeader.value = true
        }

        // Start schedulers (only if leader or no leader election)
        if (isLeader.value || leaderElector == null) {
            cronScheduler?.start()
            fixedDelayScheduler?.start()
            fixedRateScheduler?.start()
            logger.info("Schedulers started")
        }

        // Start worker pool
        if (config.enableWorkers) {
            val poolConfig = WorkerPoolConfig(
                queues = config.workerQueues,
                minWorkers = config.minWorkers,
                maxWorkers = config.maxWorkers,
                workerConcurrency = config.workerConcurrency,
                workerNamePrefix = "${config.name}-worker",
                consumerGroup = config.consumerGroup
            )

            workerPool = WorkerPool(
                config = poolConfig,
                broker = broker,
                backend = backend,
                taskRegistry = taskRegistry.toMap(),
                json = json
            )

            workerPool?.start()
            logger.info("Worker pool started with ${config.minWorkers} workers")
        }

        // Start leadership monitor
        if (leaderElector != null) {
            startLeadershipMonitor()
        }

        // Start health reporter
        startHealthReporter()

        logger.info("CeleryApp '${config.name}' started successfully")
    }

    /**
     * Stop the application gracefully.
     */
    suspend fun stop(timeout: Duration = 30.seconds) {
        if (!isRunning.compareAndSet(true, false)) {
            logger.warn("CeleryApp '${config.name}' is not running")
            return
        }

        logger.info("Stopping CeleryApp '${config.name}'...")

        withTimeout(timeout) {
            // Stop schedulers
            cronScheduler?.stop()
            fixedDelayScheduler?.stop()
            fixedRateScheduler?.stop()
            logger.info("Schedulers stopped")

            // Stop worker pool
            workerPool?.stop(timeout / 2)
            workerPool = null
            logger.info("Worker pool stopped")

            // Step down from leadership
            if (leadershipHandle != null) {
                leaderElector?.stepDown(leadershipHandle!!)
                isLeader.value = false
                logger.info("Stepped down from leadership")
            }
        }

        // Close connections
        broker.close()
        backend?.close()
        deadLetterQueue?.close()
        lock?.close()
        leaderElector?.close()

        appScope.cancel()

        logger.info("CeleryApp '${config.name}' stopped. " +
                "Tasks sent: ${tasksSent.get()}, scheduled: ${tasksScheduled.get()}")
    }

    /**
     * Check if the application is running.
     */
    fun isRunning(): Boolean = isRunning.get()

    /**
     * Check if this instance is the leader.
     */
    fun isLeader(): Boolean = isLeader.value

    /**
     * Observe leadership status.
     */
    fun observeLeadership(): StateFlow<Boolean> = isLeader.asStateFlow()

    /**
     * Get application statistics.
     */
    fun getStats(): CeleryStats {
        return CeleryStats(
            name = config.name,
            isRunning = isRunning.get(),
            isLeader = isLeader.value,
            registeredTasks = taskRegistry.size,
            tasksSent = tasksSent.get(),
            tasksScheduled = tasksScheduled.get(),
            workerStats = workerPool?.getStats()
        )
    }

    /**
     * Execute a task (called by schedulers and workers).
     */
    private suspend fun executeTask(taskName: String, context: TaskContext) {
        val task = taskRegistry[taskName]
        if (task == null) {
            logger.error("Unknown task: $taskName")
            return
        }

        try {
            task.beforeRun(context)
            val result = task.run(context)
            task.onSuccess(context, result)
            task.afterRun(context)
        } catch (e: Exception) {
            logger.error("Task execution failed: $taskName [${context.taskId}]", e)
            task.onFailure(context, e)

            // Move to dead letter if configured
            if (config.deadLetterEnabled && deadLetterQueue != null) {
                deadLetterQueue.enqueue(
                    task = TaskMessage.create(taskName = taskName),
                    reason = "Task execution failed",
                    exception = e
                )
            }
        }
    }

    /**
     * Monitor leadership and handle promotion/demotion.
     */
    private fun startLeadershipMonitor() {
        appScope.launch {
            leaderElector?.let { elector ->
                elector.onPromotion(config.leaderGroup) { handle ->
                    logger.info("Promoted to leader for group: ${config.leaderGroup}")
                    leadershipHandle = handle
                    isLeader.value = true

                    // Start schedulers on promotion
                    cronScheduler?.start()
                    fixedDelayScheduler?.start()
                    fixedRateScheduler?.start()
                }

                // Watch leadership changes
                elector.watchLeadership(config.leaderGroup).collect { leader ->
                    if (!leader && isLeader.value) {
                        logger.info("Lost leadership for group: ${config.leaderGroup}")
                        isLeader.value = false
                        leadershipHandle = null

                        // Stop schedulers on demotion
                        cronScheduler?.stop()
                        fixedDelayScheduler?.stop()
                        fixedRateScheduler?.stop()
                    }
                }
            }
        }
    }

    /**
     * Periodic health reporting.
     */
    private fun startHealthReporter() {
        appScope.launch {
            while (isActive) {
                try {
                    val stats = getStats()
                    logger.debug("Health: running=${stats.isRunning}, leader=${stats.isLeader}, " +
                            "workers=${stats.workerStats?.workerCount ?: 0}, " +
                            "tasks=${stats.registeredTasks}")
                } catch (e: Exception) {
                    logger.error("Health report failed", e)
                }
                delay(config.healthReportInterval)
            }
        }
    }

    // ==================== Builder ====================

    /**
     * Create a new builder for CeleryApp.
     */
    companion object {
        fun builder(): Builder = Builder()
    }

    /**
     * Builder for CeleryApp.
     */
    class Builder {
        private var name: String = "celery-app"
        private var broker: MessageBroker? = null
        private var backend: ResultBackend? = null
        private var lock: DistributedLock? = null
        private var leaderElector: LeaderElector? = null
        private var deadLetterQueue: DeadLetterQueue? = null
        private var json: Json = Json { ignoreUnknownKeys = true }

        // Feature flags
        private var enableCronScheduler = true
        private var enableFixedDelayScheduler = true
        private var enableFixedRateScheduler = true
        private var enableWorkers = true
        private var deadLetterEnabled = true

        // Worker config
        private var workerQueues = listOf("default")
        private var minWorkers = 2
        private var maxWorkers = 10
        private var workerConcurrency = 4
        private var consumerGroup = "celery-workers"

        // Leader config
        private var leaderGroup = "celery-leaders"
        private var leaderTtl = 30.seconds

        // Other
        private var resultExpiry = 24.minutes * 60
        private var healthReportInterval = 60.seconds

        fun withName(name: String) = apply { this.name = name }
        fun withBroker(broker: MessageBroker) = apply { this.broker = broker }
        fun withBackend(backend: ResultBackend) = apply { this.backend = backend }
        fun withLock(lock: DistributedLock) = apply { this.lock = lock }
        fun withLeaderElector(elector: LeaderElector) = apply { this.leaderElector = elector }
        fun withDeadLetterQueue(dlq: DeadLetterQueue) = apply { this.deadLetterQueue = dlq }
        fun withJson(json: Json) = apply { this.json = json }

        fun enableCronScheduler(enable: Boolean) = apply { this.enableCronScheduler = enable }
        fun enableFixedDelayScheduler(enable: Boolean) = apply { this.enableFixedDelayScheduler = enable }
        fun enableFixedRateScheduler(enable: Boolean) = apply { this.enableFixedRateScheduler = enable }
        fun enableWorkers(enable: Boolean) = apply { this.enableWorkers = enable }
        fun enableDeadLetter(enable: Boolean) = apply { this.deadLetterEnabled = enable }

        fun withWorkerQueues(queues: List<String>) = apply { this.workerQueues = queues }
        fun withWorkerCount(min: Int, max: Int) = apply {
            this.minWorkers = min
            this.maxWorkers = max
        }
        fun withWorkerConcurrency(concurrency: Int) = apply { this.workerConcurrency = concurrency }
        fun withConsumerGroup(group: String) = apply { this.consumerGroup = group }

        fun withLeaderGroup(group: String) = apply { this.leaderGroup = group }
        fun withLeaderTtl(ttl: Duration) = apply { this.leaderTtl = ttl }

        fun withResultExpiry(expiry: Duration) = apply { this.resultExpiry = expiry }
        fun withHealthReportInterval(interval: Duration) = apply { this.healthReportInterval = interval }

        fun build(): CeleryApp {
            require(broker != null) { "Broker is required" }

            val config = CeleryConfig(
                name = name,
                broker = broker,
                backend = backend,
                lock = lock,
                leaderElector = leaderElector,
                deadLetterQueue = deadLetterQueue,
                json = json,
                enableCronScheduler = enableCronScheduler,
                enableFixedDelayScheduler = enableFixedDelayScheduler,
                enableFixedRateScheduler = enableFixedRateScheduler,
                enableWorkers = enableWorkers,
                deadLetterEnabled = deadLetterEnabled,
                workerQueues = workerQueues,
                minWorkers = minWorkers,
                maxWorkers = maxWorkers,
                workerConcurrency = workerConcurrency,
                consumerGroup = consumerGroup,
                leaderGroup = leaderGroup,
                leaderTtl = leaderTtl,
                resultExpiry = resultExpiry,
                healthReportInterval = healthReportInterval
            )

            return CeleryApp(config)
        }
    }
}

// ==================== Configuration ====================

/**
 * CeleryApp configuration.
 */
data class CeleryConfig(
    val name: String,
    val broker: MessageBroker?,
    val backend: ResultBackend?,
    val lock: DistributedLock?,
    val leaderElector: LeaderElector?,
    val deadLetterQueue: DeadLetterQueue?,
    val json: Json,
    val enableCronScheduler: Boolean,
    val enableFixedDelayScheduler: Boolean,
    val enableFixedRateScheduler: Boolean,
    val enableWorkers: Boolean,
    val deadLetterEnabled: Boolean,
    val workerQueues: List<String>,
    val minWorkers: Int,
    val maxWorkers: Int,
    val workerConcurrency: Int,
    val consumerGroup: String,
    val leaderGroup: String,
    val leaderTtl: Duration,
    val resultExpiry: Duration,
    val healthReportInterval: Duration
)

// ==================== Statistics ====================

/**
 * Application statistics.
 */
data class CeleryStats(
    val name: String,
    val isRunning: Boolean,
    val isLeader: Boolean,
    val registeredTasks: Int,
    val tasksSent: Long,
    val tasksScheduled: Long,
    val workerStats: io.celery.worker.PoolStats?
)

// ==================== Scheduler Type ====================

/**
 * Type of scheduler.
 */
enum class SchedulerType {
    CRON,
    FIXED_DELAY,
    FIXED_RATE
}