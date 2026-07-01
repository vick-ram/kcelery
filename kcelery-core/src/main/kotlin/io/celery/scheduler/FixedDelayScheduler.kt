package io.celery.scheduler

import io.celery.task.TaskConfig
import io.celery.task.TaskContext
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Fixed delay scheduler.
 * Executes tasks with a fixed delay between the end of one execution 
 * and the start of the next.
 */
class FixedDelayScheduler(
    private val taskExecutor: suspend (String, TaskContext) -> Unit,
    private val clock: () -> Instant = { Instant.now() },
    private val threadPoolSize: Int = Runtime.getRuntime().availableProcessors()
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    private val schedules = ConcurrentHashMap<String, FixedDelaySchedule>()
    private val running = ConcurrentHashMap.newKeySet<String>()

    private val queue = PriorityBlockingQueue<DelayExecution>(
        11,
        compareBy { it.nextExecutionTime }
    )

    private var schedulerJob: Job? = null
    private val isRunning = AtomicBoolean(false)

    data class FixedDelaySchedule(
        val taskName: String,
        val delay: Duration,
        val config: TaskConfig,
        val lastExecution: Instant? = null
    )

    data class DelayExecution(
        val taskName: String,
        val nextExecutionTime: Instant,
        val config: TaskConfig
    ) : Comparable<DelayExecution> {
        override fun compareTo(other: DelayExecution): Int {
            return nextExecutionTime.compareTo(other.nextExecutionTime)
        }
    }

    /**
     * Schedule a task with a fixed delay.
     * The delay is measured from the END of the previous execution.
     */
    fun schedule(
        taskName: String,
        delay: Duration,
        config: TaskConfig = TaskConfig()
    ): String {
        require(delay.isPositive()) { "Delay must be positive, got: $delay" }

        val schedule = FixedDelaySchedule(taskName, delay, config)
        val taskId = "$taskName-fixed-delay-${Instant.now().toEpochMilli()}"

        schedules[taskId] = schedule

        // First execution starts after the delay
        val nextExecution = clock().plusMillis(delay.inWholeMilliseconds)
        queue.offer(DelayExecution(taskName, nextExecution, config))

        logger.info("Scheduled fixed-delay task: $taskName with delay: $delay")
        return taskId
    }

    /**
     * Update the delay for an existing schedule.
     */
    fun updateDelay(taskId: String, newDelay: Duration) {
        require(newDelay.isPositive()) { "Delay must be positive, got: $newDelay" }

        schedules.computeIfPresent(taskId) { _, schedule ->
            schedule.copy(delay = newDelay)
        }

        logger.info("Updated delay for $taskId to $newDelay")
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
            logger.warn("FixedDelayScheduler already running")
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

                        // Wait until execution time if needed
                        val delay = execution.nextExecutionTime.toEpochMilli() - now.toEpochMilli()
                        if (delay > 0) {
                            delay(delay.milliseconds)
                        }

                        launch {
                            executeTask(execution)
                        }
                    }
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.error("Error in fixed-delay scheduler loop", e)
                    delay(1.seconds)
                }
            }
        }

        logger.info("FixedDelayScheduler started")
    }

    /**
     * Stop the scheduler.
     */
    suspend fun stop() {
        isRunning.set(false)
        schedulerJob?.cancelAndJoin()
        scope.cancel()
        logger.info("FixedDelayScheduler stopped")
    }

    private suspend fun executeTask(execution: DelayExecution) {
        val taskId = execution.taskName
        val schedule = schedules.values.find { it.taskName == taskId }
            ?: return

        // Handle misfire
        val now = clock()
        if (execution.nextExecutionTime.isBefore(now.minusSeconds(1))) {
            when (execution.config.misfirePolicy) {
                MisfirePolicy.IGNORE -> {
                    logger.debug("Ignoring misfire for task: $taskId")
                    scheduleNext(taskId, now)
                    return
                }
                MisfirePolicy.FIRE_ONCE -> {
                    logger.info("Firing once for misfired task: $taskId")
                }
                MisfirePolicy.FIRE_ALL -> {
                    logger.info("Firing all for misfired task: $taskId")
                    // For fixed delay, fire once since delay is between executions
                }
            }
        }

        // Check concurrent execution
        if (!schedule.config.allowConcurrentExecution && running.contains(taskId)) {
            logger.debug("Skipping concurrent execution of: $taskId")
            scheduleNext(taskId, now)
            return
        }

        try {
            running.add(taskId)

            val context = TaskContext(
                taskId = "$taskId-${now.toEpochMilli()}",
                taskName = taskId,
                executionTime = now,
                isMisfire = execution.nextExecutionTime.isBefore(now),
                isScheduled = true
            )

            val startTime = clock()
            taskExecutor(taskId, context)
            val endTime = clock()

            // Schedule next execution based on END time (fixed delay)
            scheduleNext(taskId, endTime)

        } catch (e: Exception) {
            logger.error("Failed to execute fixed-delay task: $taskId", e)
            // Schedule next execution even on failure
            scheduleNext(taskId, clock())
        } finally {
            running.remove(taskId)
        }
    }

    private fun scheduleNext(taskId: String, afterTime: Instant) {
        val schedule = schedules.values.find { it.taskName == taskId } ?: return

        val nextExecution = afterTime.plusMillis(schedule.delay.inWholeMilliseconds)
        queue.offer(DelayExecution(taskId, nextExecution, schedule.config))

        logger.debug("Scheduled next execution of $taskId at $nextExecution")
    }

    /**
     * Get all scheduled tasks.
     */
    fun getScheduledTasks(): List<FixedDelaySchedule> = schedules.values.toList()
}