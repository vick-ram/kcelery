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
 * Central orchestrator for the kCelery framework.
 *
 * Wires together the message broker, result backend, distributed locks,
 * time-based schedulers, and multi-tenant worker pools. It acts as the primary
 * engine interface for registering handlers, dispatching asynchronous payloads,
 * and tracking cluster topology state transitions.
 *
 * ### Example Usage:
 * ```kotlin
 * val app = CeleryApp.builder()
 *      .withName("analytics-service")
 *      .withBroker(redisBroker)
 *      .withBackend(redisBackend)
 *      .withLeaderElector(redisLeaderElector)
 *      .withDeadLetterQueue(deadLetterQueue)
 *      .withWorkerCount(min = 2, max = 5)
 *      .withWorkerConcurrency(4)
 *      .withWorkerQueues(listOf("default", "high-priority", "emails"))
 *      .withResultExpiry(1.hours)
 *      .build()
 *
 * app.registerTask(computeMetricsTask)
 * app.start()
 *
 * // Enqueue asynchronous execution
 * app.sendTask("compute-metrics", args = listOf(JsonPrimitive("Q2-2026")))
 *
 * // Graceful termination
 * app.stop()
 * ```
 *
 * @property config Internal frozen snapshot structure of configuration fields.
 */
class CeleryApp private constructor(
    private val config: CeleryConfig
) {
    private val logger = LoggerFactory.getLogger(javaClass)

    /** The transport mechanism utilized to distribute raw message streams. Required**/
    private val broker: MessageBroker = config.broker
        ?: throw IllegalStateException("Broker is required")

    /** The state tracker used to save task results **/
    private val backend: ResultBackend? = config.backend

    /** Distributed coordination primitive used for standard cluster concurrency boundaries. */
    private val lock: DistributedLock? = config.lock

    /** Consensus coordinator used to determine execution authority across cluster nodes. */
    private val leaderElector: LeaderElector? = config.leaderElector

    /** Isolation sink used to contain corrupted or unparseable task messages. */
    private val deadLetterQueue: DeadLetterQueue? = config.deadLetterQueue

    /** Thread-safe hash dictionary tracking active execution signatures using their string names. */
    private val taskRegistry = ConcurrentHashMap<String, CeleryTask<*>>()

    /** Dynamic cron parser engine. Will be instantiated if cron scheduling flags are checked. */
    private val cronScheduler: CronScheduler?

    /** Interval timer loop that forces delays based on the termination threshold of the previous pass. */
    private val fixedDelayScheduler: FixedDelayScheduler?

    /** Interval loop that forces executions uniformly across strict mathematical time frames. */
    private val fixedRateScheduler: FixedRateScheduler?

    /** The active cluster worker structure wrapping concurrent polling queues. Managed at runtime. */
    private var workerPool: WorkerPool? = null

    /** Thread-safe lifecycle check evaluating if this engine instance is accepting messages. */
    private val isRunning = AtomicBoolean(false)

    /** Dedicated context boundary for supervisor structures managing sub-components. */
    private val appScope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    /** Leadership lease pointer containing renewal keys. Nullable if follower or single-node. */
    private var leadershipHandle: LockHandle? = null

    /** Internal state collector publishing active cluster leadership status updates. */
    private val isLeader = MutableStateFlow(false)

    /** Standard JSON serialization engine mapped for task arguments parsing. */
    private val json: Json = config.json

    /** Monotonically increasing counter documenting tasks dispatched from this node instance. */
    private val tasksSent = java.util.concurrent.atomic.AtomicLong(0)

    /** Monotonically increasing counter documenting recurring timers declared on this engine. */
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

    /**
     * Registers a single task handler into the execution table registry.
     *
     * @param task The concrete task instance containing user business code.
     * @throws IllegalArgumentException if an execution signature with an identical name already exists.
     */
    fun registerTask(task: CeleryTask<*>) {
        require(!taskRegistry.containsKey(task.name)){
            "Task '${task.name}' is already registered"
        }
        taskRegistry[task.name] = task
        logger.info("Registered task: ${task.name}")
    }

    /**
     * Registers a batch array of task execution handlers in bulk.
     * * @param tasks Variable length argument mapping of tasks to register.
     */
    fun registerTasks(vararg tasks: CeleryTask<*>) {
        tasks.forEach { registerTask(it) }
    }

    /**
     * Purges a task name signature from the lookup dictionary.
     * @param taskName The text label of the handler to remove.
     */
    fun unregisterTask(taskName: String) {
        taskRegistry.remove(taskName)
        logger.info("Unregistered task: $taskName")
    }

    /**
     * Checks if a task identity label is currently loaded in the memory layout.
     * @param taskName The target search parameter.
     * @return True if the descriptor is found, false otherwise.
     */
    fun isTaskRegistered(taskName: String): Boolean {
        return taskRegistry.containsKey(taskName)
    }

    /**
     * Get all registered task names.
     */
    fun getRegisteredTasks(): Set<String> = taskRegistry.keys.toSet()


    /**
     * Dispatches a complex task signature for non-blocking asynchronous execution.
     *
     * @param taskName The unique text key of the target task handler.
     * @param args Positional data parameters embedded into a JSON array layout.
     * @param kwargs Named query keyword arguments mapped inside a JSON string hash dictionary.
     * @param queue Target queue label routing path. Defaults to `"default"`.
     * @param priority Evaluation hierarchy prioritization. `0` maps as immediate attention.
     * @param delay Time window to wait before routing this record out to active consumers.
     * @param expires A terminal lifecycle window after which the message is declared stale.
     * @param headers Metadictionary blocks capturing ambient context traces.
     * @return A completed [TaskMessage] instance tracking the allocated task ID.
     * @throws IllegalArgumentException if the target task signature is unknown to this node instance.
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
     * Dispatches a task signature utilizing a simplified primitive String parameter structure.
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
     * Dispatches multiple processing records wrapped together across uniform matrix arrays.
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

    /**
     * Schedules a recurring execution signature regulated by an advanced standard Cron expression pattern.
     *
     * @param scheduleId Unique textual reference used to isolate this scheduling pointer.
     * @param taskName Target task handler mapping identity label.
     * @param cronExpression Five or six field parsing string token matching standard Unix/Quartz intervals.
     * @param config Ad-hoc local configurations override parameters.
     * @return Generated schedule string key mappings.
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
     * Schedules a recurring task loop structured using a strict pause window injected between operational iterations.
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
     * Schedules a task instance across fixed intervals, attempting to maintain execution cadence regardless of previous runtimes
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
     * Unloads a loaded schedule pointer out of processing pipelines.
     *
     * @param scheduleId Target registration key to drop.
     * @param schedulerType Strategy class context of the target schedule instance.
     */
    fun unschedule(scheduleId: String, schedulerType: SchedulerType = SchedulerType.CRON) {
        when (schedulerType) {
            SchedulerType.CRON -> cronScheduler?.unschedule(scheduleId)
            SchedulerType.FIXED_DELAY -> fixedDelayScheduler?.unschedule(scheduleId)
            SchedulerType.FIXED_RATE -> fixedRateScheduler?.unschedule(scheduleId)
        }
        logger.info("Unscheduled task: $scheduleId")
    }

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
     * Suspend-waits for a task to arrive at a terminal state, evaluating the backend at timed intervals.
     *
     * This function performs non-blocking coroutine pooling, freeing the host thread pool to process in-flight tasks.
     *
     * @param timeout Absolute time ceiling threshold before terminating the search.
     * @param pollInterval Delay frequency window injected between subsequent checks.
     * @return Captured state properties, or null if the timeout ceiling is breached.
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
     * Signals cancellation across both active tracking structures and external worker buffers.
     *
     * @return True to document the dispatch of revocation signals.
     */
    suspend fun revokeTask(taskId: String): Boolean {
        backend?.revokeTask(taskId)
        workerPool?.revokeTask(taskId)
        logger.info("Revoked task: $taskId")
        return true
    }

    /**
     * Bootstraps the application orchestrator.
     * * Initiates supervisor scopes, campaigns for cluster consensus leadership loops,
     * activates time-based scheduling pipelines, and starts thread-concurrency worker threads.
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
     * Halts all processing loops gracefully.
     *
     * Order of Operations:
     * 1. Tears down active schedulers to halt incoming task increments.
     * 2. Signals worker pools to finish in-flight records while refusing incoming ones.
     * 3. Relinquishes consensus leader tags.
     * 4. Safe-closes network connections to brokers, locks, and backend servers.
     * 5. Invalidates the parent Coroutine Scope.
     *
     * @param timeout Maximum absolute safety window allowed to drain active tasks before forcing closure.
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
        lock?.close()
        leaderElector?.close()

        appScope.cancel()

        logger.info("CeleryApp '${config.name}' stopped. " +
                "Tasks sent: ${tasksSent.get()}, scheduled: ${tasksScheduled.get()}")
    }

    /**
     * Checks if this instance is actively processing data or accepting incoming signals.
     */
    fun isRunning(): Boolean = isRunning.get()

    /**
     * Checks if this engine instance holds the active leader lock of its cluster group.
     */
    fun isLeader(): Boolean = isLeader.value

    /**
     * Returns a reactive flow channel tracking leadership promotion/demotion updates.
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
     * Executes internal hooks (`beforeRun`, `onSuccess`, `onFailure`) around a task execution signature.
     * * Catches and encapsulates target exceptions, routing failures safely to the DLQ structure if enabled.
     */
    private suspend fun <T : Any> executeSafely(task: CeleryTask<T>, context: TaskContext) {
        try {
            task.beforeRun(context)
            val result = task.run(context)   // returns T
            task.onSuccess(context, result)  // expects T — compiles cleanly
            task.afterRun(context)
        } catch (e: Exception) {
            logger.error("Task execution failed: ${task.name} [${context.taskId}]", e)
            task.onFailure(context, e)
            if (config.deadLetterEnabled && deadLetterQueue != null) {
                deadLetterQueue.enqueue(
                    task = TaskMessage.create(taskName = task.name),
                    reason = "Task execution failed",
                    exception = e
                )
            }
        }
    }

    /**
     * Matches string signatures to task registrations and passes routing contexts to [executeSafely].
     */
    private suspend fun executeTask(taskName: String, context: TaskContext) {
        val task = taskRegistry[taskName]
        if (task == null) {
            logger.error("Unknown task: $taskName")
            return
        }
        executeSafely(task, context)
    }

    /**
     * Spins up an async monitoring block watching consensus state flows to adjust local schedulers.
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
     * Establishes background coroutine routines printing performance telemetry metrics at set intervals.
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


/**
 * Type of scheduler.
 */
enum class SchedulerType {
    CRON,
    FIXED_DELAY,
    FIXED_RATE
}