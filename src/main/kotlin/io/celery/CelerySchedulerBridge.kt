package io.celery

import io.celery.core.Trigger
import kotlinx.serialization.Serializable
import io.celery.core.Clock
import io.celery.core.CronExpression
import io.celery.core.CronScheduler
import io.celery.core.DistributedLockManager
import io.celery.core.ScheduledTask
import io.celery.core.SchedulerMetrics
import io.celery.core.TaskConfig
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.time.withTimeout
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.DelayQueue
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class CelerySchedulerBridge(
    private val clock: Clock = Clock.utc(),
    private val json: Json,
    private val redis: RedisCoroutinesCommands<String, String>,
    private val lockManager: DistributedLockManager,
    private val metrics: SchedulerMetrics,
    private val broker: MessageBroker,
    private val backend: ResultBackend?
) {
    private val logger = LoggerFactory.getLogger(CelerySchedulerBridge::class.java)

    // Both system registries
    private val taskRegistry = TaskRegistry()
    private val unifiedTaskRegistry = ConcurrentHashMap<String, CeleryTask<*>>()
    private val scheduledTasks = ConcurrentHashMap<String, ScheduledTask>()

    // Execution coordination
    private val running = AtomicBoolean(false)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val taskQueue = DelayQueue<ScheduledTask>()
    private val executionChannel = Channel<UnifiedTaskExecution>(Channel.BUFFERED)

    companion object {
        private const val SCHEDULED_TASK_PREFIX = "celery:scheduled"
        private const val PERIODIC_TASK_PREFIX = "celery:periodic"
    }

    data class UnifiedTaskExecution(
        val taskId: String,
        val taskName: String,
        val args: List<JsonElement>,
        val kwargs: Map<String, JsonElement>,
        val executionTime: Instant,
        val attempt: Int,
        val isMisfire: Boolean,
        val isScheduled: Boolean,
        val queue: String = "default",
        val scheduleInfo: ScheduleInfo? = null
    )

    /**
     * Register a unified task (works with both systems)
     */
    fun registerTask(task: CeleryTask<*>) {
        unifiedTaskRegistry[task.name] = task

        // Create wrapped handler for scheduler
        val schedulerHandler: suspend (CronScheduler.TaskContext) -> Unit = { ctx ->
            val unifiedCtx = TaskContext(
                taskId = ctx.task.id,
                taskName = task.name,
                args = emptyList(),
                kwargs = emptyMap(),
                executionTime = ctx.executionTime,
                attempt = ctx.attempt,
                isMisfire = ctx.isMisfire,
                isScheduled = true
            )
            executeTask(task, unifiedCtx)
        }

        // Create Celery task wrapper for async execution
        val celeryTask = object : CeleryTask<JsonElement>(
            name = task.name,
            maxRetries = task.maxRetries,
            defaultRetryDelay = task.defaultRetryDelay,
            serializer = JsonElement.serializer() // Changed serializer to JsonElement.serializer()
        ) {
            override suspend fun run(args: List<JsonElement>, kwargs: Map<String, JsonElement>): JsonElement {
                // Call the original task's run method
                // Since `task` is `CeleryTask<*>` we need to cast it to `CeleryTask<Any?>` to call `run`
                @Suppress("UNCHECKED_CAST")
                val originalResult = (task).run(args, kwargs)
                // Now serialize originalResult using the original task's serializer
                // `task.serializer` is `KSerializer<out Any?>`. We need to cast it to `KSerializer<Any?>`.
                @Suppress("UNCHECKED_CAST")
                return json.encodeToJsonElement(task.serializer as KSerializer<Any?>, originalResult)
            }

            override suspend fun run(context: TaskContext): JsonElement {
                // Call the original task's run method with the context
                val originalResult = task.run(context)
                // Serialize the result
                @Suppress("UNCHECKED_CAST")
                return json.encodeToJsonElement(task.serializer as KSerializer<Any?>, originalResult)
            }

            override fun onRetry(exc: Exception, retries: Int): Long {
                return task.onRetry(exc, retries)
            }
        }

        taskRegistry.register(celeryTask)
    }

    /**
     * Schedule a task using cron expression
     */
    suspend fun scheduleCron(
        id: String,
        taskName: String,
        cronExpression: String,
        args: List<JsonElement> = emptyList(),
        kwargs: Map<String, JsonElement> = emptyMap(),
        config: TaskConfig = TaskConfig()
    ): Result<TaskResult> {
        val task = unifiedTaskRegistry[taskName]
            ?: return Result.failure(IllegalArgumentException("Task $taskName not registered"))

        val expression = CronExpression.parse(cronExpression)
        val trigger = Trigger.CronTrigger(expression)

        return scheduleUnifiedTask(
            id = id,
            taskName = taskName,
            trigger = trigger,
            args = args,
            kwargs = kwargs,
            config = config,
            scheduleInfo = ScheduleInfo(
                scheduleType = "cron",
                expression = cronExpression
            )
        )
    }

    /**
     * Schedule a task with fixed delay
     */
    suspend fun scheduleFixedDelay(
        id: String,
        taskName: String,
        delayMs: Long,
        args: List<JsonElement> = emptyList(),
        kwargs: Map<String, JsonElement> = emptyMap(),
        config: TaskConfig = TaskConfig()
    ): Result<TaskResult> {
        val task = unifiedTaskRegistry[taskName]
            ?: return Result.failure(IllegalArgumentException("Task $taskName not registered"))

        val trigger = Trigger.FixedDelayTrigger(delayMs)

        return scheduleUnifiedTask(
            id = id,
            taskName = taskName,
            trigger = trigger,
            args = args,
            kwargs = kwargs,
            config = config,
            scheduleInfo = ScheduleInfo(
                scheduleType = "fixed_delay",
                intervalMs = delayMs
            )
        )
    }

    /**
     * Schedule a task with fixed rate
     */
    suspend fun scheduleFixedRate(
        id: String,
        taskName: String,
        periodMs: Long,
        args: List<JsonElement> = emptyList(),
        kwargs: Map<String, JsonElement> = emptyMap(),
        config: TaskConfig = TaskConfig(),
        startTime: Instant? = null
    ): Result<TaskResult> {
        val task = unifiedTaskRegistry[taskName]
            ?: return Result.failure(IllegalArgumentException("Task $taskName not registered"))

        val start = startTime ?: clock.instant()
        val trigger = Trigger.FixedRateTrigger(periodMs, start)

        return scheduleUnifiedTask(
            id = id,
            taskName = taskName,
            trigger = trigger,
            args = args,
            kwargs = kwargs,
            config = config,
            scheduleInfo = ScheduleInfo(
                scheduleType = "fixed_rate",
                intervalMs = periodMs
            )
        )
    }

    /**
     * Send async task through Celery broker
     */
    suspend fun sendTask(
        taskName: String,
        args: List<JsonElement> = emptyList(),
        kwargs: Map<String, JsonElement> = emptyMap(),
        queue: String = "default",
        priority: Int = 0,
        countdown: Long? = null,
        eta: Instant? = null,
        expires: Long? = null,
        maxRetries: Int = 3
    ): Result<TaskMessage> {
        val task = unifiedTaskRegistry[taskName]
            ?: return Result.failure(IllegalArgumentException("Task $taskName not registered"))

        val message = TaskMessage(
            taskName = taskName,
            args = args,
            kwargs = kwargs,
            queue = queue,
            priority = priority,
            eta = eta?.toEpochMilli() ?: countdown?.let { System.currentTimeMillis() + it * 1000 },
            expires = expires?.let { System.currentTimeMillis() + it * 1000 },
            maxRetries = maxRetries
        )

        try {
            broker.publish(message, queue)

            // Store initial state
            backend?.storeResult(
                message.id,
                TaskResult(
                    taskId = message.id,
                    state = TaskState.PENDING,
                    workerName = null
                )
            )

            logger.info("Task sent: ${message.id} ($taskName)")
            return Result.success(message)
        } catch (e: Exception) {
            logger.error("Failed to send task: $taskName", e)
            return Result.failure(e)
        }
    }

    /**
     * Start the integrated system
     */
    suspend fun start(workerCount: Int = 1, workerConcurrency: Int = 4) {
        if (!running.compareAndSet(false, true)) {
            logger.warn("Bridge already running")
            return
        }

        logger.info("Starting CelerySchedulerBridge")

        // Load persisted scheduled tasks
        loadScheduledTasks()

        // Start scheduler loop
        startSchedulerLoop()

        // Start execution workers
        startExecutionWorkers()

        // Start result collector
        startResultCollector()

        logger.info("Bridge started successfully")
    }

    /**
     * Stop the integrated system
     */
    suspend fun stop(timeout: Duration = Duration.ofSeconds(30)) {
        if (!running.compareAndSet(true, false)) {
            logger.warn("Bridge not running")
            return
        }

        logger.info("Stopping CelerySchedulerBridge")

        executionChannel.close()

        withTimeout(timeout) {
            // Persist scheduled tasks
            persistScheduledTasks()

            // Cancel all jobs
            scope.cancel()
        }

        logger.info("Bridge stopped")
    }

    /**
     * Get unified task result
     */
    suspend fun getResult(taskId: String): TaskResult? {
        // Check scheduled tasks
        scheduledTasks[taskId]?.let { task ->
            return TaskResult(
                taskId = task.id,
                state = task.state,
                executionTime = task.lastExecutionTime,
                nextScheduledRun = task.nextExecutionTime,
                scheduleInfo = extractScheduleInfo(task)
            )
        }

        // Check Celery results
        val celeryResult = backend?.getResult(taskId)
        if (celeryResult != null) {
            return TaskResult(
                taskId = celeryResult.taskId,
                state = celeryResult.state,
                result = celeryResult.result,
                traceback = celeryResult.traceback,
                completedAt = celeryResult.completedAt,
                workerName = celeryResult.workerName
            )
        }

        return null
    }

    /**
     * Cancel a task (works for both scheduled and async tasks)
     */
    suspend fun cancelTask(taskId: String): Boolean {
        // Try to cancel scheduled task
        scheduledTasks.computeIfPresent(taskId) { _, task ->
            taskQueue.remove(task)
            task.copy(state = TaskState.CANCELLED)
        }

        // Revoke async task
        backend?.revokeTask(taskId)

        return true
    }

    // Private implementation

    private suspend fun scheduleUnifiedTask(
        id: String,
        taskName: String,
        trigger: Trigger,
        args: List<JsonElement>,
        kwargs: Map<String, JsonElement>,
        config: TaskConfig,
        scheduleInfo: ScheduleInfo
    ): Result<TaskResult> {
        return try {
            val now = clock.instant()
            val nextExecution = trigger.nextExecutionTime(clock)

            val scheduledTask = ScheduledTask(
                id = id,
                trigger = trigger,
                config = config,
                taskName = taskName,
                nextExecutionTime = nextExecution,
                state = TaskState.SCHEDULED,
                createdAt = now,
                updatedAt = now
            )

            // Store task metadata (args, kwargs, etc.)
            val taskData = ScheduledTaskData(
                args = args,
                kwargs = kwargs,
                scheduleInfo = scheduleInfo
            )
            redis.set("$SCHEDULED_TASK_PREFIX:$id:data", json.encodeToString(taskData))

            // Persist task
            redis.set("$SCHEDULED_TASK_PREFIX:$id", json.encodeToString(scheduledTask))

            // Add to memory
            scheduledTasks[id] = scheduledTask
            taskQueue.offer(scheduledTask)

            val result = TaskResult(
                taskId = id,
                state = TaskState.SCHEDULED,
                nextScheduledRun = nextExecution,
                scheduleInfo = scheduleInfo
            )

            logger.info("Scheduled task: $id ($taskName)")
            Result.success(result)
        } catch (e: Exception) {
            logger.error("Failed to schedule task: $id", e)
            Result.failure(e)
        }
    }

    private suspend fun executeTask(task: CeleryTask<*>, context: TaskContext) {
        val startTime = System.nanoTime()

        try {
            // Update state
            updateTaskState(context.taskId, TaskState.RUNNING)

            // Execute with timeout
            val result = withTimeout(5.minutes) {
                // Cast `task` to `CeleryTask<Any?>` to call its `run` method, which returns `Any?`.
                task.run(context)
            }

            // Record success
            val duration = Duration.ofNanos(System.nanoTime() - startTime)
            metrics.recordExecution(task.name, duration)

            // Store result
            // `task.serializer` is `KSerializer<out Any?>`. We need to cast it to `KSerializer<Any?>`.
            @Suppress("UNCHECKED_CAST")
            val serializedResult = json.encodeToJsonElement(task.serializer as KSerializer<Any?>, result)

            backend?.storeResult(
                context.taskId,
                TaskResult(
                    taskId = context.taskId,
                    state = TaskState.SUCCESS,
                    result = serializedResult,
                    completedAt = clock.instant()
                )
            )

            updateTaskState(context.taskId, TaskState.SUCCESS)

        } catch (e: Exception) {
            handleExecutionFailure(task, context, e)
        }
    }

    private suspend fun handleExecutionFailure(
        task: CeleryTask<*>,
        context: TaskContext,
        error: Throwable
    ) {
        logger.error("Task ${task.name}[${context.taskId}] failed", error)
        metrics.recordFailure(task.name, error)

        if (context.attempt < task.maxRetries) {
            val retryDelay = task.onRetry(error as? Exception ?: Exception(error), context.attempt + 1)

            // For scheduled tasks, reschedule with retry
            if (context.isScheduled) {
                val retryExecution = clock.instant().plusMillis(retryDelay * 1000)
                executionChannel.send(
                    UnifiedTaskExecution(
                        taskId = context.taskId,
                        taskName = context.taskName,
                        args = context.args,
                        kwargs = context.kwargs,
                        executionTime = retryExecution,
                        attempt = context.attempt + 1,
                        isMisfire = false,
                        isScheduled = true,
                        scheduleInfo = extractScheduleInfo(context.taskId)
                    )
                )
            } else {
                // For async tasks, republish with ETA
                broker.publish(
                    TaskMessage(
                        id = context.taskId,
                        taskName = context.taskName,
                        args = context.args,
                        kwargs = context.kwargs,
                        retries = context.attempt + 1,
                        maxRetries = task.maxRetries,
                        eta = System.currentTimeMillis() + retryDelay * 1000
                    ),
                    context.queue
                )
            }

            updateTaskState(context.taskId, TaskState.RETRY)

        } else {
            // Max retries exceeded
            task.onFailure(error as? Exception ?: Exception(error))

            backend?.storeResult(
                context.taskId,
                TaskResult(
                    taskId = context.taskId,
                    state = io.celery.TaskState.FAILURE,
                    traceback = error.stackTraceToString(),
                    completedAt = clock.instant()
                )
            )

            updateTaskState(context.taskId, TaskState.FAILURE)
        }
    }

    private suspend fun updateTaskState(taskId: String, state: TaskState) {
        scheduledTasks.computeIfPresent(taskId) { _, task ->
            task.copy(
                state = when (state) {
                    TaskState.RUNNING -> TaskState.RUNNING
                    TaskState.FAILURE -> TaskState.FAILURE
                    TaskState.CANCELLED -> TaskState.CANCELLED
                    TaskState.PAUSED -> TaskState.PAUSED
                    else -> TaskState.SCHEDULED
                },
                updatedAt = clock.instant()
            )
        }
    }

    private fun startSchedulerLoop() {
        scope.launch {
            while (running.get()) {
                try {
                    val task = withTimeout(1.seconds) {
                        taskQueue.poll(1, java.util.concurrent.TimeUnit.SECONDS)
                    }

                    if (task != null && task.state == TaskState.SCHEDULED) {
                        val now = clock.instant()

                        // Load task data
                        val taskDataJson = redis.get("$SCHEDULED_TASK_PREFIX:${task.id}:data")
                        val taskData = taskDataJson?.let {
                            json.decodeFromString<ScheduledTaskData>(it)
                        } ?: ScheduledTaskData()

                        executionChannel.send(
                            UnifiedTaskExecution(
                                taskId = task.id,
                                taskName = task.taskName,
                                args = taskData.args,
                                kwargs = taskData.kwargs,
                                executionTime = now,
                                attempt = 0,
                                isMisfire = task.nextExecutionTime?.isBefore(now) == true,
                                isScheduled = true,
                                scheduleInfo = taskData.scheduleInfo
                            )
                        )

                        // Reschedule
                        rescheduleTask(task)
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.error("Error in scheduler loop", e)
                    delay(1.seconds)
                }
            }
        }
    }

    private fun startExecutionWorkers() {
        repeat(Runtime.getRuntime().availableProcessors()) { workerId ->
            scope.launch(Dispatchers.Default) {
                for (execution in executionChannel) {
                    try {
                        val task = unifiedTaskRegistry[execution.taskName]
                        if (task != null) {
                            val context = TaskContext(
                                taskId = execution.taskId,
                                taskName = execution.taskName,
                                args = execution.args,
                                kwargs = execution.kwargs,
                                executionTime = execution.executionTime,
                                attempt = execution.attempt,
                                isMisfire = execution.isMisfire,
                                isScheduled = execution.isScheduled,
                                workerName = "bridge-worker-$workerId",
                                queue = execution.queue
                            )
                            executeTask(task, context)
                        } else {
                            logger.error("No handler for task: ${execution.taskName}")
                        }
                    } catch (e: Exception) {
                        logger.error("Worker $workerId failed", e)
                    }
                }
            }
        }
    }

    private fun startResultCollector() {
        scope.launch {
            while (running.get()) {
                try {
                    // Periodic cleanup of old results
                    delay(60.seconds)
                    logger.debug("Result collector heartbeat")
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.error("Error in result collector", e)
                }
            }
        }
    }

    private suspend fun rescheduleTask(task: ScheduledTask) {
        scheduledTasks.computeIfPresent(task.id) { _, existingTask ->
            if (existingTask.state !in setOf(TaskState.CANCELLED, TaskState.FAILURE)) {
                val nextExecution = existingTask.trigger.nextExecutionTime(clock)
                val rescheduled = existingTask.copy(
                    nextExecutionTime = nextExecution,
                    state = TaskState.SCHEDULED,
                    version = existingTask.version + 1,
                    updatedAt = clock.instant()
                )
                taskQueue.offer(rescheduled)

                // Persist
                scope.launch {
                    redis.set("$SCHEDULED_TASK_PREFIX:${rescheduled.id}", json.encodeToString(rescheduled))
                }

                rescheduled
            } else {
                existingTask
            }
        }
    }

    private suspend fun loadScheduledTasks() {
        try {
            val keys = redis.keys("$SCHEDULED_TASK_PREFIX:*")
            keys.filter { !it.endsWith(":data") }.collect { key ->
                try {
                    val taskJson = redis.get(key)
                    if (taskJson != null) {
                        val task = json.decodeFromString<ScheduledTask>(taskJson)
                        if (task.state == TaskState.SCHEDULED) {
                            scheduledTasks[task.id] = task
                            taskQueue.offer(task)
                        }
                    }
                } catch (e: Exception) {
                    logger.error("Failed to load task: $key", e)
                }
            }
            logger.info("Loaded ${scheduledTasks.size} scheduled tasks")
        } catch (e: Exception) {
            logger.error("Failed to load scheduled tasks", e)
        }
    }

    private suspend fun persistScheduledTasks() {
        scheduledTasks.values.forEach { task ->
            try {
                redis.set("$SCHEDULED_TASK_PREFIX:${task.id}", json.encodeToString(task))
            } catch (e: Exception) {
                logger.error("Failed to persist task: ${task.id}", e)
            }
        }
    }

    private fun extractScheduleInfo(task: ScheduledTask): ScheduleInfo? {
        return when (val trigger = task.trigger) {
            is Trigger.CronTrigger -> ScheduleInfo(
                scheduleType = "cron",
                expression = trigger.expression.toString()
            )
            is Trigger.FixedDelayTrigger -> ScheduleInfo(
                scheduleType = "fixed_delay",
                intervalMs = trigger.delayMs
            )
            is Trigger.FixedRateTrigger -> ScheduleInfo(
                scheduleType = "fixed_rate",
                intervalMs = trigger.periodMs
            )
        }
    }

    private fun extractScheduleInfo(taskId: String): ScheduleInfo? {
        return scheduledTasks[taskId]?.let { extractScheduleInfo(it) }
    }

    @Serializable
    private data class ScheduledTaskData(
        val args: List<JsonElement> = emptyList(),
        val kwargs: Map<String, JsonElement> = emptyMap(),
        val scheduleInfo: ScheduleInfo? = null
    )
}