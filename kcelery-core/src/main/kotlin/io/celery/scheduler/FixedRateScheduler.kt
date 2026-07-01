package io.celery.scheduler

import io.celery.task.TaskConfig
import io.celery.task.TaskContext
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.max
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Fixed rate scheduler.
 * Executes tasks at a fixed rate, regardless of execution time.
 * If execution takes longer than the period, the next execution starts immediately.
 */
class FixedRateScheduler(
    private val taskExecutor: suspend (String, TaskContext) -> Unit,
    private val clock: () -> Instant = { Instant.now() },
    private val threadPoolSize: Int = Runtime.getRuntime().availableProcessors()
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    private val schedules = ConcurrentHashMap<String, FixedRateSchedule>()
    private val running = ConcurrentHashMap.newKeySet<String>()

    private val queue = PriorityBlockingQueue<RateExecution>(
        11,
        compareBy { it.nextExecutionTime }
    )

    private var schedulerJob: Job? = null
    private val isRunning = AtomicBoolean(false)

    data class FixedRateSchedule(
        val taskName: String,
        val period: Duration,
        val config: TaskConfig,
        val startTime: Instant,
        val executionCount: Long = 0
    )

    data class RateExecution(
        val taskName: String,
        val nextExecutionTime: Instant,
        val config: TaskConfig,
        val executionNumber: Long
    ) : Comparable<RateExecution> {
        override fun compareTo(other: RateExecution): Int {
            return nextExecutionTime.compareTo(other.nextExecutionTime)
        }
    }

    /**
     * Schedule a task at a fixed rate.
     * The rate is measured from the START of each execution.
     */
    fun schedule(
        taskName: String,
        period: Duration,
        config: TaskConfig = TaskConfig(),
        startTime: Instant? = null
    ): String {
        require(period.isPositive()) { "Period must be positive, got: $period" }

        val start = startTime ?: clock()
        val schedule = FixedRateSchedule(taskName, period, config, start)
        val taskId = "$taskName-fixed-rate-${Instant.now().toEpochMilli()}"

        schedules[taskId] = schedule

        // Calculate first execution
        val now = clock()
        val firstExecution = if (now.isBefore(start)) {
            start
        } else {
            // Calculate next execution based on start time
            val elapsed = now.toEpochMilli() - start.toEpochMilli()
            val periods = max(0, elapsed / period.inWholeMilliseconds) + 1
            start.plusMillis(periods * period.inWholeMilliseconds)
        }

        val executionNumber = max(0,
            (firstExecution.toEpochMilli() - start.toEpochMilli()) / period.inWholeMilliseconds()
        )

        queue.offer(RateExecution(taskName, firstExecution, config, executionNumber))

        logger.info("Scheduled fixed-rate task: $taskName with period: $period")
        return taskId
    }

    /**
     * Update the period for an existing schedule.
     */
    fun updatePeriod(taskId: String, newPeriod: Duration) {
        require(newPeriod.isPositive()) { "Period must be positive, got: $newPeriod" }

        schedules.computeIfPresent(taskId) { _, schedule ->
            schedule.copy(period = newPeriod)
        }

        logger.info("Updated period for $taskId to $newPeriod")
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
            logger.warn("FixedRateScheduler already running")
            return
        }

        schedulerJob = scope.launch {
            while (isActive && isRunning.get()) {
                try {
                    val execution = withTimeoutOrNull(1.seconds) {
                        queue.take()
                    }

                    if (execution != null) {
                        val now = clock()

                        // Wait until execution time
                        val delayMs = execution.nextExecutionTime.toEpochMilli() - now.toEpochMilli()
                        if (delayMs > 0) {
                            delay(delayMs.milliseconds)
                        }

                        launch {
                            executeTask(execution)
                        }

                        // Schedule next execution immediately (fixed rate)
                        scheduleNext(execution)
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.error("Error in fixed-rate scheduler loop", e)
                    delay(1.seconds)
                }
            }
        }

        logger.info("FixedRateScheduler started")
    }

    /**
     * Stop the scheduler.
     */
    suspend fun stop() {
        isRunning.set(false)
        schedulerJob?.cancelAndJoin()
        scope.cancel()
        logger.info("FixedRateScheduler stopped")
    }

    private suspend fun executeTask(execution: RateExecution) {
        val taskId = execution.taskName
        val schedule = schedules.values.find { it.taskName == taskId }
            ?: return

        // Handle misfire
        val now = clock()
        val isMisfire = execution.nextExecutionTime.isBefore(now.minusSeconds(1))

        if (isMisfire) {
            when (execution.config.misfirePolicy) {
                MisfirePolicy.IGNORE -> {
                    logger.debug("Ignoring misfire for task: $taskId")
                    return
                }
                MisfirePolicy.FIRE_ONCE -> {
                    logger.info("Firing once for misfired task: $taskId")
                }
                MisfirePolicy.FIRE_ALL -> {
                    logger.info("Firing all missed for task: $taskId")
                    // Fire all missed executions
                    var missedTime = execution.nextExecutionTime
                    val period = schedule.period
                    while (missedTime.isBefore(now)) {
                        fireTask(taskId, missedTime, execution.config, true, execution.executionNumber)
                        missedTime = missedTime.plusMillis(period.inWholeMilliseconds)
                    }
                    return
                }
            }
        }

        fireTask(taskId, execution.nextExecutionTime, execution.config, isMisfire, execution.executionNumber)
    }

    private suspend fun fireTask(
        taskId: String,
        executionTime: Instant,
        config: TaskConfig,
        isMisfire: Boolean,
        executionNumber: Long
    ) {
        // Check concurrent execution
        if (!config.allowConcurrentExecution && running.contains(taskId)) {
            logger.debug("Skipping concurrent execution of: $taskId")
            return
        }

        try {
            running.add(taskId)

            val context = TaskContext(
                taskId = "$taskId-${executionTime.toEpochMilli()}",
                taskName = taskId,
                executionTime = executionTime,
                isMisfire = isMisfire,
                isScheduled = true
            )

            taskExecutor(taskId, context)

            // Update execution count
            schedules.computeIfPresent(taskId) { _, schedule ->
                schedule.copy(executionCount = executionNumber + 1)
            }

        } catch (e: Exception) {
            logger.error("Failed to execute fixed-rate task: $taskId", e)
        } finally {
            running.remove(taskId)
        }
    }

    private fun scheduleNext(execution: RateExecution) {
        val schedule = schedules.values.find { it.taskName == execution.taskName } ?: return

        val nextExecutionNumber = execution.executionNumber + 1
        val nextExecutionTime = schedule.startTime.plusMillis(
            nextExecutionNumber * schedule.period.inWholeMilliseconds
        )

        queue.offer(RateExecution(
            execution.taskName,
            nextExecutionTime,
            execution.config,
            nextExecutionNumber
        ))
    }

    /**
     * Get all scheduled tasks.
     */
    fun getScheduledTasks(): List<FixedRateSchedule> = schedules.values.toList()

    /**
     * Get pending executions.
     */
    fun getPendingExecutions(): List<RateExecution> = queue.toList()
}