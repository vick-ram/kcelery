package io.celery.scheduler

import io.celery.test.RedisTestBase
import io.celery.core.MisfirePolicy
import io.celery.core.TaskConfig
import io.celery.metrics.SchedulerMetrics
import io.celery.model.TaskState
import io.celery.scheduler.CronScheduler
import io.celery.trigger.Trigger
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import java.time.Instant
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class CronSchedulerTest : RedisTestBase() {

    private lateinit var scheduler: CronScheduler
    private val meterRegistry = SimpleMeterRegistry()

    @OptIn(ExperimentalLettuceCoroutinesApi::class)
    @BeforeEach
    override fun setUp() {
        super.setUp()

        val metrics = SchedulerMetrics(meterRegistry)
        scheduler = CronScheduler(
            clock = clock,
            workerThreads = 2,
            json = json,
            redis = redisCommands,
            lockManager = lockManager,
            metrics = metrics
        )
    }

    @Test
    fun `register and execute a simple task`() = runTest {
        val latch = CountDownLatch(1)
        var executionTime: Instant? = null

        scheduler.registerTask("test-task") { context ->
            executionTime = context.executionTime
            latch.countDown()
        }

        scheduler.start()

        // Schedule task to run immediately
        scheduler.scheduleCron(
            id = "immediate-task",
            cronExpression = "* * * * * *", // Every second
            taskName = "test-task"
        )

        assertTrue(latch.await(5, TimeUnit.SECONDS))
        assertNotNull(executionTime)

        scheduler.stop()
    }

    @Test
    fun `schedule multiple tasks`() = runTest {
        val executionCount = java.util.concurrent.atomic.AtomicInteger(0)

        scheduler.registerTask("counter") {
            executionCount.incrementAndGet()
        }

        scheduler.start()

        scheduler.scheduleCron("task-1", "*/1 * * * * *", "counter")
        scheduler.scheduleCron("task-2", "*/1 * * * * *", "counter")

        delay(3.seconds)

        assertTrue(executionCount.get() >= 4, "Expected at least 4 executions, got ${executionCount.get()}")

        scheduler.stop()
    }

    @Test
    fun `cancel task should prevent execution`() = runTest {
        val executionCount = java.util.concurrent.atomic.AtomicInteger(0)

        scheduler.registerTask("should-not-run") {
            executionCount.incrementAndGet()
        }

        scheduler.start()

        scheduler.scheduleCron("to-cancel", "*/1 * * * * *", "should-not-run")
        delay(500.milliseconds)

        scheduler.cancel("to-cancel")
        delay(2.seconds)

        assertEquals(0, executionCount.get(), "Task should not have executed after cancellation")

        scheduler.stop()
    }

    @Test
    fun `pause and resume task`() = runTest {
        val executionCount = java.util.concurrent.atomic.AtomicInteger(0)

        scheduler.registerTask("pausable") {
            executionCount.incrementAndGet()
        }

        scheduler.start()

        scheduler.scheduleCron("pause-test", "*/1 * * * * *", "pausable")
        delay(1.5.seconds)

        val firstCount = executionCount.get()

        scheduler.pause("pause-test")
        delay(2.seconds)

        assertEquals(firstCount, executionCount.get(), "Task should not execute while paused")

        scheduler.resume("pause-test")
        delay(1.5.seconds)

        assertTrue(executionCount.get() > firstCount, "Task should resume execution")

        scheduler.stop()
    }

    @Test
    fun `task with max retries should retry on failure`() = runTest {
        val attempts = java.util.concurrent.atomic.AtomicInteger(0)
        val maxRetries = 2

        scheduler.registerTask("flaky-task") { context ->
            attempts.incrementAndGet()
            throw RuntimeException("Simulated failure")
        }

        scheduler.start()

        scheduler.schedule(
            id = "flaky",
            trigger = Trigger.FixedDelayTrigger(100),
            taskName = "flaky-task",
            config = TaskConfig(
                maxRetries = maxRetries,
                retryDelayMs = 100
            )
        )

        delay(3.seconds)

        assertTrue(attempts.get() > 1, "Task should have retried")

        scheduler.stop()
    }

    @Test
    fun `task should not execute concurrently when disabled`() = runTest {
        val concurrentCount = java.util.concurrent.atomic.AtomicInteger(0)
        val maxConcurrent = java.util.concurrent.atomic.AtomicInteger(0)

        scheduler.registerTask("sequential") {
            val current = concurrentCount.incrementAndGet()
            maxConcurrent.updateAndGet { maxOf(it, current) }
            delay(500.milliseconds) // Simulate work
            concurrentCount.decrementAndGet()
        }

        scheduler.start()

        scheduler.scheduleFixedRate(
            id = "sequential-test",
            periodMs = 100,
            taskName = "sequential",
            config = TaskConfig(allowConcurrentExecution = false)
        )

        delay(3.seconds)

        assertEquals(1, maxConcurrent.get(), "Should not have concurrent executions")

        scheduler.stop()
    }

    @Test
    fun `misfire policy IGNORE should skip missed executions`() = runTest {
        val executionTimes = mutableListOf<Instant>()

        scheduler.registerTask("misfire-test") { context ->
            executionTimes.add(context.executionTime)
        }

        scheduler.start()

        // Schedule task that should have fired in the past
        scheduler.schedule(
            id = "misfire",
            trigger = Trigger.FixedRateTrigger(
                1000,
                clock.instant().minusSeconds(3600) // Started an hour ago
            ),
            taskName = "misfire-test",
            config = TaskConfig(misfirePolicy = MisfirePolicy.IGNORE)
        )

        delay(3.seconds)

        // Should only have recent executions, not all missed ones
        assertTrue(executionTimes.size <= 5, "Should ignore most missed executions")

        scheduler.stop()
    }

    @Test
    fun `get task status should return correct state`() = runTest {
        scheduler.registerTask("status-check") { }
        scheduler.start()

        scheduler.scheduleCron("check-status", "0 0 12 * * *", "status-check")

        val task = scheduler.getTask("check-status")
        assertNotNull(task)
        assertEquals(TaskState.SCHEDULED, task?.state)

        scheduler.cancel("check-status")

        val cancelledTask = scheduler.getTask("check-status")
        assertEquals(TaskState.CANCELLED, cancelledTask?.state)

        scheduler.stop()
    }

    @Test
    fun `get all tasks should return all registered tasks`() {
        scheduler.registerTask("task-1") { }
        scheduler.registerTask("task-2") { }

        runTest {
            scheduler.start()

            scheduler.scheduleCron("id-1", "0 0 * * * *", "task-1")
            scheduler.scheduleCron("id-2", "0 0 * * * *", "task-2")

            val allTasks = scheduler.getAllTasks()
            assertEquals(2, allTasks.size)

            scheduler.stop()
        }
    }

    @Test
    fun `task timeout should be enforced`() = runTest {
        val started = CountDownLatch(1)
        val completed = java.util.concurrent.atomic.AtomicBoolean(false)

        scheduler.registerTask("slow-task") {
            started.countDown()
            delay(5.seconds) // Takes 5 seconds
            completed.set(true)
        }

        scheduler.start()

        scheduler.schedule(
            id = "timeout-test",
            trigger = Trigger.FixedDelayTrigger(100),
            taskName = "slow-task",
            config = TaskConfig(timeoutMs = 500) // 500ms timeout
        )

        assertTrue(started.await(2, TimeUnit.SECONDS))
        delay(2.seconds)

        assertFalse(completed.get(), "Task should have timed out")

        scheduler.stop()
    }
}