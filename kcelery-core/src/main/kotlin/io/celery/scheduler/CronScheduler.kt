package io.celery.scheduler

import io.celery.task.TaskConfig
import io.celery.task.TaskContext
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Cron-based task scheduler.
 * Handles execution of tasks on cron schedules.
 */
class CronScheduler(
    private val taskExecutor: suspend (String, TaskContext) -> Unit,
    private val clock: () -> Instant = { Instant.now() },
    private val threadPoolSize: Int = Runtime.getRuntime().availableProcessors()
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    private val schedules = ConcurrentHashMap<String, CronSchedule>()
    private val running = ConcurrentHashMap.newKeySet<String>()

    private val queue = PriorityBlockingQueue<CronExecution>(
        11,
        compareBy { it.nextExecutionTime }
    )

    private var schedulerJob: Job? = null
    private val isRunning = AtomicBoolean(false)

    data class CronSchedule(
        val taskName: String,
        val expression: CronExpressionParser,
        val config: TaskConfig,
        val lastExecution: Instant? = null
    )

    data class CronExecution(
        val taskName: String,
        val nextExecutionTime: Instant,
        val config: TaskConfig
    ) : Comparable<CronExecution> {
        override fun compareTo(other: CronExecution): Int {
            return nextExecutionTime.compareTo(other.nextExecutionTime)
        }
    }

    /**
     * Schedule a task with a cron expression.
     */
    fun schedule(
        taskName: String,
        cronExpression: String,
        config: TaskConfig = TaskConfig()
    ): String {
        val expression = CronExpressionParser.parse(cronExpression)
        val schedule = CronSchedule(taskName, expression, config)
        val taskId = "$taskName-${Instant.now().toEpochMilli()}"

        schedules[taskId] = schedule

        val nextExecution = calculateNextExecution(schedule)
        queue.offer(CronExecution(taskName, nextExecution, config))

        logger.info("Scheduled cron task: $taskName with expression: $cronExpression")
        return taskId
    }

    /**
     * Unschedule a task.
     */
    fun unschedule(taskId: String) {
        schedules.remove(taskId)
        logger.info("Unscheduled task: $taskId")
    }

    /**
     * Start the scheduler.
     */
    fun start() {
        if (!isRunning.compareAndSet(false, true)) {
            logger.warn("Scheduler already running")
            return
        }

        schedulerJob = scope.launch {
            while (isActive && isRunning.get()) {
                try {
                    val execution = withTimeoutOrNull(1.seconds) {
                        queue.take()
                    }

                    if (execution != null) {
                        launch {
                            executeTask(execution)
                        }
                    }

                    // Check for new schedules
                    cleanupCompletedTasks()
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.error("Error in scheduler loop", e)
                    delay(1.seconds)
                }
            }
        }

        logger.info("CronScheduler started")
    }

    /**
     * Stop the scheduler.
     */
    suspend fun stop() {
        isRunning.set(false)
        schedulerJob?.cancelAndJoin()
        scope.cancel()
        logger.info("CronScheduler stopped")
    }

    private suspend fun executeTask(execution: CronExecution) {
        val now = clock()

        // Handle misfire
        if (execution.nextExecutionTime.isBefore(now)) {
            when (execution.config.misfirePolicy) {
                MisfirePolicy.IGNORE -> {
                    logger.debug("Ignoring misfire for task: ${execution.taskName}")
                    return
                }
                MisfirePolicy.FIRE_ONCE -> {
                    logger.info("Firing once for misfired task: ${execution.taskName}")
                }
                MisfirePolicy.FIRE_ALL -> {
                    logger.info("Firing all for misfired task: ${execution.taskName}")
                    // Fire all missed executions
                    var missedTime = execution.nextExecutionTime
                    while (missedTime.isBefore(now)) {
                        fireTask(execution.taskName, missedTime, execution.config, true)
                        // Calculate next missed time (simplified)
                        missedTime = missedTime.plusSeconds(60)
                    }
                    return
                }
            }
        }

        fireTask(execution.taskName, execution.nextExecutionTime, execution.config, false)
    }

    private suspend fun fireTask(
        taskName: String,
        executionTime: Instant,
        config: TaskConfig,
        isMisfire: Boolean
    ) {
        if (!config.allowConcurrentExecution && running.contains(taskName)) {
            logger.debug("Skipping concurrent execution of: $taskName")
            return
        }

        try {
            running.add(taskName)

            val context = TaskContext(
                taskId = "$taskName-${executionTime.toEpochMilli()}",
                taskName = taskName,
                executionTime = executionTime,
                isMisfire = isMisfire,
                isScheduled = true
            )

            taskExecutor(taskName, context)
        } catch (e: Exception) {
            logger.error("Failed to execute scheduled task: $taskName", e)
        } finally {
            running.remove(taskName)

            // Reschedule
            val schedule = schedules.values.find { it.taskName == taskName }
            if (schedule != null) {
                val nextExecution = calculateNextExecution(schedule)
                queue.offer(CronExecution(taskName, nextExecution, config))
            }
        }
    }

    private fun calculateNextExecution(schedule: CronSchedule): Instant {
        val now = clock()
        val zonedNow = java.time.ZonedDateTime.ofInstant(now, java.time.ZoneOffset.UTC)
        val nextZoned = schedule.expression.nextMatchAfter(zonedNow)
        return nextZoned.toInstant()
    }

    private fun cleanupCompletedTasks() {
        // Periodic cleanup of stale schedules if needed
    }

    /**
     * Get all scheduled tasks.
     */
    fun getScheduledTasks(): List<CronSchedule> = schedules.values.toList()

    /**
     * Get pending executions.
     */
    fun getPendingExecutions(): List<CronExecution> = queue.toList()
}