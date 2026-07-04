package io.celery.scheduler

import io.celery.task.TaskConfig
import io.celery.task.TaskContext
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.max
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class FixedRateScheduler(
    private val taskExecutor: suspend (String, TaskContext) -> Unit,
    private val clock: () -> Instant = { Instant.now() }
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    private val schedules = ConcurrentHashMap<String, FixedRateSchedule>()
    private val running = ConcurrentHashMap.newKeySet<String>()
    private val executionJobs = ConcurrentHashMap<String, Job>()

    private val wakeUpSignal = Channel<Unit>(Channel.CONFLATED)
    private var schedulerJob: Job? = null
    private val isRunning = AtomicBoolean(false)

    data class FixedRateSchedule(
        val taskId: String,
        val taskName: String,
        val period: Duration,
        val config: TaskConfig,
        val startTime: Instant,
        val executionCount: Long,
        val nextExecutionTime: Instant
    )

    fun schedule(
        taskName: String,
        period: Duration,
        config: TaskConfig = TaskConfig(),
        startTime: Instant? = null
    ): String {
        require(period.isPositive()) { "Period must be positive, got: $period" }

        val start = startTime ?: clock()
        val now = clock()
        val taskId = "$taskName-fixed-rate-${Instant.now().toEpochMilli()}"

        // Calculate first alignment execution target cleanly
        val firstExecution = if (now.isBefore(start)) {
            start
        } else {
            val elapsed = now.toEpochMilli() - start.toEpochMilli()
            val periods = (elapsed / period.inWholeMilliseconds) + 1
            start.plusMillis(periods * period.inWholeMilliseconds)
        }

        val initialExecutionNumber = max(0L, (firstExecution.toEpochMilli() - start.toEpochMilli()) / period.inWholeMilliseconds)

        schedules[taskId] = FixedRateSchedule(
            taskId = taskId,
            taskName = taskName,
            period = period,
            config = config,
            startTime = start,
            executionCount = initialExecutionNumber,
            nextExecutionTime = firstExecution
        )

        logger.info("Scheduled fixed-rate task '$taskName' (id=$taskId) period=$period first=$firstExecution")
        wakeUpSignal.trySend(Unit)
        return taskId
    }

    fun updatePeriod(taskId: String, newPeriod: Duration) {
        require(newPeriod.isPositive()) { "Period must be positive, got: $newPeriod" }

        schedules.computeIfPresent(taskId) { _, old ->
            val now = clock()
            // Reset base timeline coordinates cleanly to match new calculations seamlessly
            val elapsed = now.toEpochMilli() - old.startTime.toEpochMilli()
            val periods = (elapsed / newPeriod.inWholeMilliseconds) + 1
            val nextTime = old.startTime.plusMillis(periods * newPeriod.inWholeMilliseconds)

            old.copy(
                period = newPeriod,
                nextExecutionTime = nextTime
            )
        }
        logger.info("Updated period for task '$taskId' to $newPeriod")
        wakeUpSignal.trySend(Unit)
    }

    fun unschedule(taskId: String) {
        schedules.remove(taskId)
        logger.info("Unscheduled task '$taskId'")
        wakeUpSignal.trySend(Unit)
    }

    fun start() {
        if (!isRunning.compareAndSet(false, true)) {
            logger.warn("FixedRateScheduler already running")
            return
        }

        schedulerJob = scope.launch {
            while (isActive && isRunning.get()) {
                try {
                    val now = clock()
                    val tasksToLaunch = mutableListOf<Pair<FixedRateSchedule, Instant>>()

                    val dueEntries = schedules.entries
                        .filter { (_, s) -> !s.nextExecutionTime.isAfter(now) }

                    for (entry in dueEntries) {
                        val taskId = entry.key
                        val scheduledFireTime = entry.value.nextExecutionTime

                        schedules.computeIfPresent(taskId) { _, current ->
                            tasksToLaunch.add(current to scheduledFireTime)

                            // Advance the execution tracking schedule atomically inside loop thread
                            val nextCount = current.executionCount + 1
                            val nextTime = current.startTime.plusMillis(nextCount * current.period.inWholeMilliseconds)
                            current.copy(executionCount = nextCount, nextExecutionTime = nextTime)
                        }
                    }

                    // Fire off all validated jobs asynchronously
                    for ((schedule, scheduledTime) in tasksToLaunch) {
                        val execKey = "${schedule.taskId}-${now.toEpochMilli()}"
                        val job = launch { executeTask(schedule, scheduledTime) }

                        executionJobs[execKey] = job
                        job.invokeOnCompletion { executionJobs.remove(execKey) }
                    }

                    // Dynamic sleep block
                    val nextWakeMs = schedules.values.minOfOrNull { it.nextExecutionTime.toEpochMilli() }
                    val sleepMs = if (nextWakeMs != null) {
                        (nextWakeMs - clock().toEpochMilli()).coerceIn(10L, 60_000L)
                    } else {
                        60_000L
                    }

                    withTimeoutOrNull(sleepMs.milliseconds) {
                        wakeUpSignal.receive()
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

    suspend fun stop() {
        isRunning.set(false)
        schedulerJob?.cancelAndJoin()
        // Wait for all in-flight asynchronous executions gracefully before scope teardown
        executionJobs.values.joinAll()
        executionJobs.clear()
        scope.cancel()
        logger.info("FixedRateScheduler stopped")
    }

    private suspend fun executeTask(schedule: FixedRateSchedule, scheduledFireTime: Instant) {
        val taskId = schedule.taskId
        if (!schedules.containsKey(taskId)) return

        val now = clock()
        val isMisfire = scheduledFireTime.isBefore(now.minusSeconds(1))

        if (isMisfire) {
            when (schedule.config.misfirePolicy) {
                MisfirePolicy.IGNORE -> {
                    logger.debug("Ignoring misfire for fixed-rate task '$taskId'")
                    return
                }
                MisfirePolicy.FIRE_ONCE -> {
                    logger.info("Firing once for misfired fixed-rate task '$taskId'")
                    fireTask(schedule, now, isMisfire = true)
                    return
                }
                MisfirePolicy.FIRE_ALL -> {
                    logger.info("Firing all missed iterations sequentially for task '$taskId'")
                    var missedTime = scheduledFireTime
                    while (missedTime.isBefore(now)) {
                        fireTask(schedule, missedTime, isMisfire = true)
                        missedTime = missedTime.plusMillis(schedule.period.inWholeMilliseconds)
                    }
                    return
                }
            }
        }

        fireTask(schedule, scheduledFireTime, isMisfire = false)
    }

    private suspend fun fireTask(schedule: FixedRateSchedule, executionTime: Instant, isMisfire: Boolean) {
        val taskId = schedule.taskId
        if (!schedule.config.allowConcurrentExecution && running.contains(taskId)) {
            logger.debug("Skipping concurrent execution of fixed-rate task '$taskId'")
            return
        }

        running.add(taskId)
        try {
            taskExecutor(
                schedule.taskName,
                TaskContext(
                    taskId = "$taskId-${executionTime.toEpochMilli()}",
                    taskName = schedule.taskName,
                    executionTime = executionTime,
                    isMisfire = isMisfire,
                    isScheduled = true
                )
            )
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            logger.error("Failed to execute fixed-rate task '$taskId'", e)
        } finally {
            running.remove(taskId)
        }
    }

    fun getScheduledTasks(): List<FixedRateSchedule> = schedules.values.toList()
}