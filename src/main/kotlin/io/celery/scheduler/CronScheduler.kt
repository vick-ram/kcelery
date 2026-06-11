package io.celery.scheduler

import io.celery.model.TaskState
import io.celery.config.InstantSerializer
import io.celery.core.Clock
import io.celery.core.MisfirePolicy
import io.celery.metrics.SchedulerMetrics
import io.celery.core.TaskConfig
import io.celery.core.ScheduledTask
import io.celery.redis.DistributedLockManager
import io.celery.trigger.CronExpression
import io.celery.trigger.Trigger
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.micrometer.core.instrument.Metrics
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.DelayQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.math.pow
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalLettuceCoroutinesApi::class, ExperimentalCoroutinesApi::class)
class CronScheduler(
    private val clock: Clock = Clock.utc(),
    private val workerThreads: Int = Runtime.getRuntime().availableProcessors(),
    private val json: Json,
    private val redis: RedisCoroutinesCommands<String, String>,
    private val lockManager: DistributedLockManager,
    private val metrics: SchedulerMetrics = SchedulerMetrics(Metrics.globalRegistry),
    private val schedulerName: String = "default"
) {
    private val logger = LoggerFactory.getLogger("celery.scheduler.$schedulerName")

    // Core state
    private val tasks = ConcurrentHashMap<String, ScheduledTask>()
    private val taskQueue = DelayQueue<ScheduledTask>()
    private val taskRegistry = ConcurrentHashMap<String, suspend (TaskContext) -> Unit>()
    private val taskLocks = ConcurrentHashMap<String, Mutex>()

    // Lifecycle
    private val running = AtomicBoolean(false)
    private val started = AtomicBoolean(false)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val dispatcher = Dispatchers.Default.limitedParallelism(workerThreads)
    private val concurrencyGuard = Semaphore(workerThreads)

    // Coordination
    private val versionCounter = AtomicLong(0)
    private val schedulerJob = CompletableDeferred<Job>()
    private val housekeepingJob = CompletableDeferred<Job>()

    // Channels for decoupled operations
    private val executionChannel = Channel<TaskExecution>(Channel.BUFFERED)
    private val persistenceChannel = Channel<ScheduledTask>(Channel.UNLIMITED)

    companion object {
        private const val TASK_KEY_PREFIX = "celery:scheduled_task"
        private const val DEAD_LETTER_PREFIX = "celery:dead_letter"
        private const val LEADER_KEY = "scheduler:leader"
        private const val MAX_TASKS = 10000
        private const val HOUSEKEEPING_INTERVAL_MS = 60_000L
    }

    /**
     * Task execution context
     */
    data class TaskContext(
        val task: ScheduledTask,
        val executionTime: Instant,
        val attempt: Int = 0,
        val isMisfire: Boolean = false
    )

    /**
     * Internal execution representation
     */
    private data class TaskExecution(
        val task: ScheduledTask,
        val executionTime: Instant,
        val attempt: Int = 0,
        val isMisfire: Boolean = false
    )

    init {
        require(workerThreads > 0) { "workerThreads must be positive" }
    }

    /**
     * Register a task handler
     */
    fun registerTask(name: String, handler: suspend (TaskContext) -> Unit) {
        require(name.isNotBlank()) { "Task name cannot be blank" }
        taskRegistry[name] = handler
        logger.info("Registered task handler: $name")
    }

    /**
     * Schedule a task with explicit trigger
     */
    suspend fun schedule(
        id: String,
        trigger: Trigger,
        taskName: String,
        config: TaskConfig = TaskConfig()
    ): Result<ScheduledTask> {
        require(id.isNotBlank()) { "Task ID cannot be blank" }
        require(taskName.isNotBlank()) { "Task name cannot be blank" }
        require(taskRegistry.containsKey(taskName)) { "No handler registered for task: $taskName" }
        require(tasks.size < MAX_TASKS) { "Maximum task limit reached: $MAX_TASKS" }

        return try {
            val version = versionCounter.incrementAndGet()
            val now = clock.instant()

            val newTask = ScheduledTask(
                id = id,
                trigger = trigger,
                config = config,
                taskName = taskName,
                version = version,
                createdAt = now,
                updatedAt = now
            ).withNextExecution(clock)

            // Atomic operation with Redis
            val success = lockManager.withLock("schedule:$id") {
                // Check if already exists
                val existingJson = redis.get("$TASK_KEY_PREFIX:$id")
                if (existingJson != null) {
                    logger.warn("Task $id already exists, updating")
                }

                // Persist to Redis
                redis.set("$TASK_KEY_PREFIX:$id", json.encodeToString(newTask))

                // Update in-memory state
                tasks.compute(id) { _, oldTask ->
                    oldTask?.let { taskQueue.remove(it) }
                    newTask
                }

                // Add to execution queue
                taskQueue.offer(newTask)

                newTask
            }

            if (success != null) {
                logger.info("Scheduled task: $id ($taskName)")
                Result.success(success)
            } else {
                Result.failure(IllegalStateException("Failed to acquire lock for scheduling"))
            }
        } catch (e: Exception) {
            logger.error("Failed to schedule task: $id", e)
            Result.failure(e)
        }
    }

    /**
     * Schedule with cron expression
     */
    suspend fun scheduleCron(
        id: String,
        cronExpression: String,
        taskName: String,
        config: TaskConfig = TaskConfig()
    ): Result<ScheduledTask> {
        return try {
            val expression = CronExpression.parse(cronExpression)
            schedule(id, Trigger.CronTrigger(expression), taskName, config)
        } catch (e: IllegalArgumentException) {
            Result.failure(e)
        }
    }

    /**
     * Schedule with fixed delay
     */
    suspend fun scheduleFixedDelay(
        id: String,
        delayMs: Long,
        taskName: String,
        config: TaskConfig = TaskConfig()
    ): Result<ScheduledTask> {
        require(delayMs > 0) { "Delay must be positive" }
        return schedule(id, Trigger.FixedDelayTrigger(delayMs), taskName, config)
    }

    /**
     * Schedule with fixed rate
     */
    suspend fun scheduleFixedRate(
        id: String,
        periodMs: Long,
        taskName: String,
        config: TaskConfig = TaskConfig(),
        startTime: Instant? = null
    ): Result<ScheduledTask> {
        require(periodMs > 0) { "Period must be positive" }
        val start = startTime ?: clock.instant()
        return schedule(id, Trigger.FixedRateTrigger(periodMs, start), taskName, config)
    }

    /**
     * Cancel a task atomically
     */
    suspend fun cancel(id: String): Boolean {
        return lockManager.withLock("cancel:$id") {
            tasks.computeIfPresent(id) { _, task ->
                taskQueue.remove(task)
                scope.launch {
                    try {
                        redis.del("$TASK_KEY_PREFIX:$id")
                    } catch (e: Exception) {
                        logger.error("Failed to delete task from Redis: $id", e)
                    }
                }
                val cancelledTask = task.copy(
                    state = TaskState.CANCELLED,
                    updatedAt = clock.instant()
                )
                logger.info("Cancelled task: $id")
                cancelledTask
            }
            true
        } ?: false
    }

    /**
     * Pause a task
     */
    suspend fun pause(id: String): Boolean {
        return lockManager.withLock("pause:$id") {
            tasks.computeIfPresent(id) { _, task ->
                taskQueue.remove(task)
                val pausedTask = task.copy(
                    state = TaskState.PAUSED,
                    nextExecutionTime = null,
                    updatedAt = clock.instant()
                )
                scope.launch {
                    try {
                        redis.set("$TASK_KEY_PREFIX:$id", json.encodeToString(pausedTask))
                    } catch (e: Exception) {
                        logger.error("Failed to persist paused task: $id", e)
                    }
                }
                logger.info("Paused task: $id")
                pausedTask
            }
            true
        } ?: false
    }

    /**
     * Resume a paused task
     */
    suspend fun resume(id: String): Boolean {
        return lockManager.withLock("resume:$id") {
            tasks.computeIfPresent(id) { _, task ->
                if (task.state == TaskState.PAUSED) {
                    val resumedTask = task.withNextExecution(clock)
                    scope.launch {
                        try {
                            redis.set("$TASK_KEY_PREFIX:$id", json.encodeToString(resumedTask))
                        } catch (e: Exception) {
                            logger.error("Failed to persist resumed task: $id", e)
                        }
                    }
                    taskQueue.offer(resumedTask)
                    logger.info("Resumed task: $id")
                    resumedTask
                } else {
                    task
                }
            }
            true
        } ?: false
    }

    /**
     * Start the scheduler
     */
    suspend fun start() {
        if (!started.compareAndSet(false, true)) {
            logger.warn("Scheduler already started")
            return
        }

        logger.info("Starting scheduler: $schedulerName")

        // Acquire leadership
        val isLeader = lockManager.tryAcquireLeadership("$LEADER_KEY:$schedulerName", 30.seconds)
        if (!isLeader) {
            logger.info("Another instance is the leader, running in standby mode")
        }

        // Start persistence worker
        startPersistenceWorker()

        // Load existing tasks
        loadTasksFromRedis()

        // Start main scheduler loop
        running.set(true)
        startSchedulerLoop()

        // Start housekeeping
        startHousekeeping()

        // Start execution workers
        startExecutionWorkers()

        logger.info("Scheduler started: $schedulerName (leader: $isLeader)")
    }

    /**
     * Stop the scheduler gracefully
     */
    suspend fun stop(timeout: Duration = 10.seconds) {
        if (!started.compareAndSet(true, false)) {
            logger.warn("Scheduler not started")
            return
        }

        logger.info("Stopping scheduler: $schedulerName")
        running.set(false)

        // Close channels
        executionChannel.close()
        persistenceChannel.close()

        // Cancel jobs
        schedulerJob.getCompleted().cancel()
        housekeepingJob.getCompleted().cancel()

        // Wait for running tasks
        withTimeout(timeout) {
            repeat(workerThreads) {
                concurrencyGuard.acquire()
            }
        }

        // Persist current state
        persistAllTasks()

        scope.cancel()
        logger.info("Scheduler stopped: $schedulerName")
    }

    /**
     * Get task status
     */
    fun getTask(id: String): ScheduledTask? = tasks[id]

    /**
     * Get all tasks
     */
    fun getAllTasks(): List<ScheduledTask> = tasks.values.toList()

    /**
     * Get task statistics
     */
    fun getStats(): SchedulerStats {
        return SchedulerStats(
            totalTasks = tasks.size,
            scheduledTasks = tasks.count { it.value.state == TaskState.SCHEDULED },
            runningTasks = tasks.count { it.value.state == TaskState.RUNNING },
            pausedTasks = tasks.count { it.value.state == TaskState.PAUSED },
            failedTasks = tasks.count { it.value.state == TaskState.FAILURE },
            queueSize = taskQueue.size
        )
    }

    // Private implementation methods

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    private suspend fun loadTasksFromRedis() {
        try {
            val keys = redis.keys("$TASK_KEY_PREFIX:*")
            keys.collect { key ->
                try {
                    val taskJson = redis.get(key)
                    if (taskJson != null) {
                        val task = json.decodeFromString<ScheduledTask>(taskJson)
                        if (task.state == TaskState.SCHEDULED) {
                            tasks[task.id] = task
                            taskQueue.offer(task)
                        }
                    }
                } catch (e: Exception) {
                    logger.error("Failed to load task from Redis: $key", e)
                }
            }
            logger.info("Loaded ${tasks.size} tasks from Redis")
        } catch (e: Exception) {
            logger.error("Failed to load tasks from Redis", e)
        }
    }

    private fun startSchedulerLoop() {
        schedulerJob.complete(scope.launch {
            while (running.get()) {
                try {
                    val task = withTimeout(1.seconds) {
                        taskQueue.poll(1, TimeUnit.SECONDS)
                    }

                    if (task != null && task.state == TaskState.SCHEDULED) {
                        val now = clock.instant()

                        // Check for misfire
                        if (task.nextExecutionTime?.isBefore(now) == true) {
                            handleMisfire(task, now)
                        } else {
                            executionChannel.send(TaskExecution(task, now))
                        }
                    }

                    metrics.updateQueueSize(taskQueue.size)
                } catch (e: TimeoutCancellationException) {
                    // Normal timeout, continue
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.error("Error in scheduler loop", e)
                    delay(1.seconds)
                }
            }
        })
    }

    private fun startExecutionWorkers() {
        repeat(workerThreads) { workerId ->
            scope.launch(dispatcher) {
                for (execution in executionChannel) {
                    try {
                        concurrencyGuard.withPermit {
                            executeTask(execution.task, execution.executionTime, execution.attempt, execution.isMisfire)
                        }
                    } catch (e: Exception) {
                        logger.error("Worker $workerId failed to execute task: ${execution.task.id}", e)
                    }
                }
            }
        }
    }

    private suspend fun executeTask(
        task: ScheduledTask,
        executionTime: Instant,
        attempt: Int = 0,
        isMisfire: Boolean = false
    ) {
        val taskId = task.id
        val taskName = task.taskName

        // Check if task still exists and is in valid state
        val currentTask = tasks[taskId]
        if (currentTask == null || currentTask.state != TaskState.SCHEDULED) {
            logger.debug("Task $taskId no longer scheduled, skipping")
            return
        }

        // Check for concurrent execution
        if (!task.config.allowConcurrentExecution) {
            val mutex = taskLocks.computeIfAbsent(taskId) { Mutex() }
            if (!mutex.tryLock()) {
                logger.debug("Task $taskId already running, skipping")
                metrics.recordSkip(taskName, "concurrent")
                rescheduleTask(currentTask)
                return
            }

            try {
                executeTaskInternal(currentTask, executionTime, attempt, isMisfire)
            } finally {
                mutex.unlock()
                // Clean up mutex after 5 minutes of inactivity
                scope.launch {
                    delay(5.minutes)
                    taskLocks.remove(taskId)
                }
            }
        } else {
            executeTaskInternal(currentTask, executionTime, attempt, isMisfire)
        }
    }

    private suspend fun executeTaskInternal(
        task: ScheduledTask,
        executionTime: Instant,
        attempt: Int,
        isMisfire: Boolean
    ) {
        val startTime = System.nanoTime()

        try {
            // Update state to RUNNING
            tasks.computeIfPresent(task.id) { _, existingTask ->
                if (existingTask.version == task.version) {
                    existingTask.copy(
                        state = TaskState.RUNNING,
                        lastExecutionTime = executionTime
                    )
                } else {
                    existingTask
                }
            }

            // Get handler
            val handler = taskRegistry[task.taskName]
            if (handler == null) {
                logger.error("No handler registered for task: ${task.taskName}")
                metrics.recordFailure(task.taskName, IllegalStateException("No handler"))
                markTaskFailed(task.id)
                return
            }

            // Execute with timeout
            val context = TaskContext(task, executionTime, attempt, isMisfire)
            withTimeout(task.config.timeoutMs.milliseconds) {
                handler(context)
            }

            // Record success
            val duration = java.time.Duration.ofNanos(System.nanoTime() - startTime)
            metrics.recordExecution(task.taskName, duration)

            // Reschedule if not cancelled
            rescheduleTask(task)

        } catch (e: TimeoutCancellationException) {
            logger.warn("Task ${task.id} timed out after ${task.config.timeoutMs}ms")
            metrics.recordFailure(task.taskName, e)
            handleRetry(task, executionTime, attempt, e)

        } catch (e: CancellationException) {
            throw e

        } catch (e: Exception) {
            logger.error("Task ${task.id} failed", e)
            metrics.recordFailure(task.taskName, e)
            handleRetry(task, executionTime, attempt, e)
        }
    }

    private suspend fun handleRetry(
        task: ScheduledTask,
        executionTime: Instant,
        attempt: Int,
        error: Throwable
    ) {
        if (attempt < task.config.maxRetries) {
            val retryDelay = calculateRetryDelay(task.config, attempt)
            logger.info("Retrying task ${task.id} in ${retryDelay}ms (attempt ${attempt + 1}/${task.config.maxRetries})")
            metrics.recordRetry(task.taskName, attempt + 1)

            delay(retryDelay.milliseconds)
            executionChannel.send(TaskExecution(task, executionTime, attempt + 1))
        } else {
            logger.error("Task ${task.id} exhausted all retries (${task.config.maxRetries})")
            markTaskFailed(task.id)

            // Send to dead letter queue if enabled
            if (task.config.deadLetterEnabled) {
                moveToDeadLetter(task, error)
            }
        }
    }

    private fun calculateRetryDelay(config: TaskConfig, attempt: Int): Long {
        val delay = config.retryDelayMs * config.retryBackoffMultiplier.pow(attempt.toDouble()).toLong()
        return minOf(delay, config.maxRetryDelayMs)
    }

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    private suspend fun markTaskFailed(taskId: String) {
        tasks.computeIfPresent(taskId) { _, task ->
            task.copy(state = TaskState.FAILURE, updatedAt = clock.instant()).also {
                scope.launch {
                    try {
                        redis.set("$TASK_KEY_PREFIX:$taskId", json.encodeToString(it))
                    } catch (e: Exception) {
                        logger.error("Failed to persist failed task state: $taskId", e)
                    }
                }
            }
        }
    }

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    private suspend fun moveToDeadLetter(task: ScheduledTask, error: Throwable) {
        try {
            val deadLetter = DeadLetter(
                originalTask = task,
                error = error.message ?: "Unknown error",
                errorType = error.javaClass.name,
                timestamp = clock.instant()
            )
            redis.set("$DEAD_LETTER_PREFIX:${task.id}:${clock.millis()}", json.encodeToString(deadLetter))
            logger.info("Task ${task.id} moved to dead letter queue")
        } catch (e: Exception) {
            logger.error("Failed to move task to dead letter queue: ${task.id}", e)
        }
    }

    private suspend fun handleMisfire(task: ScheduledTask, currentTime: Instant) {
        when (task.config.misfirePolicy) {
            MisfirePolicy.IGNORE -> {
                logger.debug("Ignoring misfire for task: ${task.id}")
                metrics.recordSkip(task.taskName, "misfire_ignored")
                rescheduleTask(task)
            }

            MisfirePolicy.FIRE_ONCE -> {
                logger.info("Firing once for misfired task: ${task.id}")
                executionChannel.send(TaskExecution(task, currentTime, isMisfire = true))
                rescheduleTask(task)
            }

            MisfirePolicy.FIRE_ALL -> {
                logger.info("Firing all missed executions for task: ${task.id}")
                var missedTime = task.nextExecutionTime
                var iterations = 0

                while (missedTime != null &&
                    missedTime.isBefore(currentTime) &&
                    iterations < 1000) {

                    executionChannel.send(TaskExecution(
                        task.copy(nextExecutionTime = missedTime),
                        missedTime,
                        isMisfire = true
                    ))

                    // Calculate next missed time
                    missedTime = task.trigger.nextExecutionTime(
                        Clock.fixed(missedTime, clock.zone()),
                        missedTime
                    )

                    iterations++
                }

                if (iterations >= 1000) {
                    logger.error("Too many misfires for task ${task.id}, stopping")
                }

                rescheduleTask(task)
            }
        }
    }

    private suspend fun rescheduleTask(task: ScheduledTask) {
        tasks.computeIfPresent(task.id) { _, existingTask ->
            if (existingTask.state !in setOf(TaskState.CANCELLED, TaskState.SUCCESS)) {
                val rescheduled = existingTask.withNextExecution(clock)
                taskQueue.offer(rescheduled)
                scope.launch {  persistenceChannel.send(rescheduled) }
                rescheduled
            } else {
                existingTask
            }
        }
    }

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    private fun startPersistenceWorker() {
        scope.launch(Dispatchers.IO) {
            for (task in persistenceChannel) {
                try {
                    redis.set("$TASK_KEY_PREFIX:${task.id}", json.encodeToString(task))
                } catch (e: Exception) {
                    logger.error("Failed to persist task: ${task.id}", e)
                }
            }
        }
    }

    private fun startHousekeeping() {
        housekeepingJob.complete(scope.launch {
            while (running.get()) {
                try {
                    delay(HOUSEKEEPING_INTERVAL_MS.milliseconds)
                    performHousekeeping()
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.error("Error during housekeeping", e)
                }
            }
        })
    }

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    private suspend fun performHousekeeping() {
        val now = clock.instant()
        val staleThreshold = now.minusSeconds(3600) // 1 hour

        // Clean up stale tasks
        tasks.entries.removeIf { (id, task) ->
            when (task.state) {
                TaskState.CANCELLED, TaskState.SUCCESS -> {
                    if (task.updatedAt.isBefore(staleThreshold)) {
                        scope.launch {
                            try {
                                redis.del("$TASK_KEY_PREFIX:$id")
                            } catch (e: Exception) {
                                logger.error("Failed to delete stale task from Redis: $id", e)
                            }
                        }
                        taskLocks.remove(id)
                        true
                    } else false
                }
                TaskState.FAILURE -> {
                    if (task.updatedAt.isBefore(staleThreshold.minusSeconds(7200))) {
                        scope.launch {
                            try {
                                redis.del("$TASK_KEY_PREFIX:$id")
                            } catch (e: Exception) {
                                logger.error("Failed to delete failed task from Redis: $id", e)
                            }
                        }
                        taskLocks.remove(id)
                        true
                    } else false
                }
                else -> false
            }
        }

        logger.debug("Housekeeping complete: ${tasks.size} active tasks")
    }

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    private suspend fun persistAllTasks() {
        tasks.values.forEach { task ->
            try {
                redis.set("$TASK_KEY_PREFIX:${task.id}", json.encodeToString(task))
            } catch (e: Exception) {
                logger.error("Failed to persist task during shutdown: ${task.id}", e)
            }
        }
    }
}

/**
 * Dead letter representation
 */
@Serializable
data class DeadLetter(
    val originalTask: ScheduledTask,
    val error: String,
    val errorType: String,
    @Serializable(with = InstantSerializer::class)
    val timestamp: Instant
)

/**
 * Scheduler statistics
 */
@Serializable
data class SchedulerStats(
    val totalTasks: Int,
    val scheduledTasks: Int,
    val runningTasks: Int,
    val pausedTasks: Int,
    val failedTasks: Int,
    val queueSize: Int
)