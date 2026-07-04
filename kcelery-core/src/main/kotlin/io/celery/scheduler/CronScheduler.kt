package io.celery.scheduler

import io.celery.task.TaskConfig
import io.celery.task.TaskContext
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class CronScheduler(
    private val taskExecutor: suspend (String, TaskContext) -> Unit,
    private val clock: () -> Instant = { Instant.now() }
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    private val schedules = ConcurrentHashMap<String, CronSchedule>()
    private val running = ConcurrentHashMap.newKeySet<String>()
    private val executionJobs = ConcurrentHashMap<String, Job>()

    // Conflated signaling channel to interrupt long sleep windows instantly
    private val wakeUpSignal = Channel<Unit>(Channel.CONFLATED)

    private var schedulerJob: Job? = null
    private val isRunning = AtomicBoolean(false)

    // Kept completely immutable to prevent cross-thread exposure issues
    data class CronSchedule(
        val taskId: String,
        val taskName: String,
        val expression: CronExpressionParser,
        val config: TaskConfig,
        val nextExecutionTime: Instant
    )

    fun schedule(
        taskName: String,
        cronExpression: String,
        config: TaskConfig = TaskConfig()
    ): String {
        val expression = CronExpressionParser.parse(cronExpression)
        val taskId = "$taskName-${Instant.now().toEpochMilli()}"
        val nextExecution = nextAfter(expression, clock())

        schedules[taskId] = CronSchedule(
            taskId = taskId,
            taskName = taskName,
            expression = expression,
            config = config,
            nextExecutionTime = nextExecution
        )

        logger.info("Scheduled cron task '$taskName' (id=$taskId) next=$nextExecution")
        wakeUpSignal.trySend(Unit)
        return taskId
    }

    fun unschedule(taskId: String) {
        schedules.remove(taskId)
        logger.info("Unscheduled task '$taskId'")
        wakeUpSignal.trySend(Unit)
    }

    fun start() {
        if (!isRunning.compareAndSet(false, true)) {
            logger.warn("CronScheduler already running")
            return
        }

        schedulerJob = scope.launch {
            while (isActive && isRunning.get()) {
                try {
                    val now = clock()
                    val tasksToLaunch = mutableListOf<Pair<CronSchedule, Instant>>()

                    // Scan the state structure safely
                    val dueEntries = schedules.entries
                        .filter { (_, s) -> !s.nextExecutionTime.isAfter(now) }

                    for (entry in dueEntries) {
                        val taskId = entry.key
                        val scheduledTime = entry.value.nextExecutionTime

                        schedules.computeIfPresent(taskId) { _, current ->
                            // 1. Snapshot the actual current data state
                            tasksToLaunch.add(current to scheduledTime)

                            // 2. Advance time ATOMICALLY inside loop thread to prevent multi-fire storms
                            val nextSlot = nextAfter(current.expression, now)
                            current.copy(nextExecutionTime = nextSlot)
                        }
                    }

                    // 3. Spawning execution contexts safely outside state mutations
                    for ((task, scheduledTime) in tasksToLaunch) {
                        val execKey = "${task.taskId}-${now.toEpochMilli()}"
                        val job = launch { executeTask(task, scheduledTime) }

                        executionJobs[execKey] = job
                        job.invokeOnCompletion { executionJobs.remove(execKey) }
                    }

                    // 4. Sleep dynamically until the next task is due or up to 60s max
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
                    logger.error("Error in cron scheduler loop", e)
                    delay(1.seconds)
                }
            }
        }

        logger.info("CronScheduler started")
    }

    suspend fun stop() {
        isRunning.set(false)
        schedulerJob?.cancelAndJoin()
        executionJobs.values.forEach { it.join() }
        executionJobs.clear()
        scope.cancel()
        logger.info("CronScheduler stopped")
    }

    fun getScheduledTasks(): List<CronSchedule> = schedules.values.toList()

    private suspend fun executeTask(schedule: CronSchedule, scheduledFireTime: Instant) {
        val taskId = schedule.taskId
        if (!schedules.containsKey(taskId)) return

        val now = clock()
        val isMisfire = scheduledFireTime.isBefore(now.minusSeconds(1))

        if (isMisfire) {
            when (schedule.config.misfirePolicy) {
                MisfirePolicy.IGNORE -> {
                    logger.debug("Ignoring misfire for '${schedule.taskName}'")
                    return
                }
                MisfirePolicy.FIRE_ONCE -> {
                    logger.info("Firing once for misfired task '${schedule.taskName}'")
                    fireTask(schedule.taskName, now, schedule.config, isMisfire = true)
                    return
                }
                MisfirePolicy.FIRE_ALL -> {
                    logger.info("Firing all missed slots for '${schedule.taskName}'")
                    var missedSlot = scheduledFireTime
                    while (!missedSlot.isAfter(now)) {
                        fireTask(schedule.taskName, missedSlot, schedule.config, isMisfire = true)

                        val nextSlot = nextAfter(schedule.expression, missedSlot)
                        // Safety switch against infinite loop if parser doesn't shift window forward
                        if (!nextSlot.isAfter(missedSlot)) {
                            logger.error("Cron parser failed to increment forward. Breaking to prevent lockup.")
                            break
                        }
                        missedSlot = nextSlot
                    }
                    return
                }
            }
        }

        fireTask(schedule.taskName, scheduledFireTime, schedule.config, isMisfire = false)
    }

    private suspend fun fireTask(
        taskName: String,
        executionTime: Instant,
        config: TaskConfig,
        isMisfire: Boolean
    ) {
        if (!config.allowConcurrentExecution && running.contains(taskName)) {
            logger.debug("Skipping concurrent execution of '$taskName'")
            return
        }

        running.add(taskName)
        try {
            taskExecutor(
                taskName,
                TaskContext(
                    taskId        = "$taskName-${executionTime.toEpochMilli()}",
                    taskName      = taskName,
                    executionTime = executionTime,
                    isMisfire     = isMisfire,
                    isScheduled   = true
                )
            )
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            logger.error("Failed to execute cron task '$taskName'", e)
        } finally {
            running.remove(taskName)
        }
    }

    private fun nextAfter(expression: CronExpressionParser, after: Instant): Instant {
        val zoned = java.time.ZonedDateTime.ofInstant(after, java.time.ZoneOffset.UTC)
        return expression.nextMatchAfter(zoned).toInstant()
    }
}