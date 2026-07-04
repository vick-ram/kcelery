package io.celery.scheduler

import io.celery.task.TaskConfig
import io.celery.task.TaskContext
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
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
    private val clock: () -> Instant = { Instant.now() }
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    private val schedules = ConcurrentHashMap<String, FixedDelaySchedule>()

    // Tracks taskIds currently executing so allowCurrentExecution can be enforced
    private val running = ConcurrentHashMap.newKeySet<String>()

    // Active execution jobs so stop() can wait for them to finish gracefully
    private val executionJobs = ConcurrentHashMap<String, Job>()

    // Used to immediately wake up the scheduler loop when things change
    private val wakeUpSignal = Channel<Unit>(Channel.CONFLATED)

    private var schedulerJob: Job? = null
    private val isRunning = AtomicBoolean(false)

    data class FixedDelaySchedule(
        val taskId: String,
        val taskName: String,
        val delay: Duration,
        val config: TaskConfig,
        val nextExecutionTime: Instant  // next scheduled fire time; updated after each execution
    )

    /**
     * Schedule a task with a fixed delay between executions.
     * The delay is measured from the END of the previous execution.
     * Returns a stable taskId to use for [updateDelay] or [unschedule].
     */
    fun schedule(
        taskName: String,
        delay: Duration,
        config: TaskConfig = TaskConfig()
    ): String {
        require(delay.isPositive()) { "Delay must be positive, got: $delay" }

        val taskId = "$taskName-fixed-delay-${Instant.now().toEpochMilli()}"
        schedules[taskId] = FixedDelaySchedule(
            taskId = taskId,
            taskName = taskName,
            delay = delay,
            config = config,
            nextExecutionTime = clock().plusMillis(delay.inWholeMilliseconds)
        )

        logger.info("Scheduled fixed-delay task: $taskName with delay: $delay")
        wakeUpSignal.trySend(Unit) // Wake loop to recalibrate for the new task
        return taskId
    }

    /**
     * Update the delay for an existing schedule. Takes effect from the
     * *next* scheduled execution — the current nextExecutionTime is
     * recalculated immediately from now so it isn't stuck at the old offset.
     */
    fun updateDelay(taskId: String, newDelay: Duration) {
        require(newDelay.isPositive()) { "Delay must be positive, got: $newDelay" }

        schedules.computeIfPresent(taskId) { _, old ->
            old.copy(
                delay = newDelay,
                nextExecutionTime = clock().plusMillis(newDelay.inWholeMilliseconds)
            )
        }
        logger.info("Updated delay for '$taskId' to $newDelay")
        wakeUpSignal.trySend(Unit)
    }

    /**
     * Unschedule a task.
     */
    fun unschedule(taskId: String) {
        schedules.remove(taskId)
        logger.info("Unscheduled task: $taskId")
        wakeUpSignal.trySend(Unit)
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
                    val now = clock()
                    var targetTask: FixedDelaySchedule? = null

                    // Find the next due task without blocking a thread
                    val due = schedules.entries
                        .filter { (_, s) -> !s.nextExecutionTime.isAfter(now) }
                        .minByOrNull { (_, s) -> s.nextExecutionTime }

                    if (due != null) {
                        val taskId = due.key
                        schedules.computeIfPresent(taskId) { _, current ->
                            targetTask = current
                            // Move next fire time deep into the future placeholder-style
                            // to break execution storms before launching asynchronously
                            current.copy(nextExecutionTime = Instant.MAX)
                        }
                    }

                    if (targetTask != null) {
                        val task = targetTask
                        val execJobKey = "${task?.taskId}-${now.toEpochMilli()}"

                        val job = launch { executeTask(task!!, now) }

                        executionJobs[execJobKey] = job
                        job.invokeOnCompletion { executionJobs.remove(execJobKey) }
                    } else {
                        // 2. Compute dynamic, interruptible sleep bounds
                        val nextWake = schedules.values.minOfOrNull { it.nextExecutionTime.toEpochMilli() }
                        val sleepMs = if (nextWake != null) {
                            (nextWake - clock().toEpochMilli()).coerceIn(50L, 60_000L)
                        } else 60_000L

                        // Race guard sleep: wakes up on signal OR timeout expiration
                        withTimeoutOrNull(sleepMs.milliseconds) {
                            wakeUpSignal.receive()
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
        // Wait for all in-flight task executions to complete gracefully
        executionJobs.values.joinAll()
        executionJobs.clear()
        scope.cancel()
        logger.info("FixedDelayScheduler stopped")
    }

    private suspend fun executeTask(schedule: FixedDelaySchedule, scheduledFireTime: Instant) {
        val taskId = schedule.taskId

        // Guard: Was it removed while the loop evaluated it?
        if (!schedules.containsKey(taskId)) return

        val now = clock()

        // Misfire check (Evaluate against scheduledFireTime, not placeholder MAX)
        if (scheduledFireTime.isBefore(now.minusSeconds(1))) {
            when (schedule.config.misfirePolicy) {
                MisfirePolicy.IGNORE -> {
                    logger.debug("Ignoring misfire for '$taskId'")
                    advanceNextExecution(taskId, now, schedule.delay)
                    return
                }
                else -> { /* Fall through to execution step */ }
            }
        }

        // Concurrent lock guard
        if (!schedule.config.allowConcurrentExecution && running.contains(taskId)) {
            logger.debug("Skipping concurrent execution of '$taskId'")
            advanceNextExecution(taskId, now, schedule.delay)
            return
        }

        running.add(taskId)
        try {
            val context = TaskContext(
                taskId = "$taskId-${now.toEpochMilli()}",
                taskName = schedule.taskName,
                executionTime = now,
                isMisfire = scheduledFireTime.isBefore(now),
                isScheduled = true
            )

            taskExecutor(schedule.taskName, context)
            advanceNextExecution(taskId, clock(), schedule.delay)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            logger.error("Failed to execute fixed-delay task '$taskId'", e)
            advanceNextExecution(taskId, clock(), schedule.delay)
        } finally {
            running.remove(taskId)
        }
    }

    private fun advanceNextExecution(taskId: String, fromTime: Instant, delay: Duration) {
        val nextRealFireTime = fromTime.plusMillis(delay.inWholeMilliseconds)
        schedules.computeIfPresent(taskId) { _, s ->
            s.copy(nextExecutionTime = nextRealFireTime)
        }
        wakeUpSignal.trySend(Unit)
    }

    /**
     * Get all scheduled tasks.
     */
    fun getScheduledTasks(): List<FixedDelaySchedule> = schedules.values.toList()
}