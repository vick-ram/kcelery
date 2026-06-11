package io.celery.core

import io.celery.test.TestBase
import io.celery.trigger.CronExpression
import io.celery.trigger.Trigger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant
import java.util.concurrent.TimeUnit

@OptIn(ExperimentalCoroutinesApi::class)
class ScheduledTaskTest : TestBase() {

    private fun createTask(
        id: String = "test-task-1",
        nextExecution: Instant = clock.instant().plusSeconds(60)
    ): ScheduledTask {
        return ScheduledTask(
            id = id,
            trigger = Trigger.CronTrigger(CronExpression.parse("0 0 * * * *")),
            config = TaskConfig(),
            taskName = "test-task",
            nextExecutionTime = nextExecution
        )
    }

    @Test
    fun `task should report correct delay`() {
        val futureTime = clock.instant().plusSeconds(30)
        val task = createTask(nextExecution = futureTime)

        val delay = task.getDelay(TimeUnit.MILLISECONDS)

        assertTrue(delay > 0)
        assertTrue(delay <= 30_000)
    }

    @Test
    fun `task should report zero delay for past execution`() {
        val pastTime = clock.instant().minusSeconds(30)
        val task = createTask(nextExecution = pastTime)

        val delay = task.getDelay(TimeUnit.MILLISECONDS)

        org.junit.jupiter.api.Assertions.assertEquals(0, delay)
    }

    @Test
    fun `task without next execution should return max delay`() {
        val task = ScheduledTask(
            id = "test",
            trigger = Trigger.CronTrigger(CronExpression.parse("0 0 * * * *")),
            config = TaskConfig(),
            taskName = "test"
        )

        val delay = task.getDelay(TimeUnit.MILLISECONDS)

        assertEquals(Long.MAX_VALUE, delay)
    }

    @Test
    fun `tasks should compare by next execution time`() {
        val task1 = createTask("task-1", clock.instant().plusSeconds(60))
        val task2 = createTask("task-2", clock.instant().plusSeconds(30))
        val task3 = createTask("task-3", clock.instant().plusSeconds(90))

        assertTrue(task1 > task2) // task2 executes sooner
        assertTrue(task1 < task3) // task3 executes later
        assertEquals(0, task1.compareTo(task1))
    }

    @Test
    fun `task with next execution should update correctly`() {
        val task = createTask()
        val updated = task.withNextExecution(clock)

        assertNotNull(updated.nextExecutionTime)
        assertTrue(updated.version > task.version)
        assertTrue(updated.updatedAt.isAfter(task.updatedAt))
    }

    @Test
    fun `serialization of scheduled task`() {
        val task = createTask()
        val json = kotlinx.serialization.json.Json.encodeToString(
            ScheduledTask.serializer(), task
        )

        val deserialized = kotlinx.serialization.json.Json.decodeFromString(
            ScheduledTask.serializer(), json
        )

        assertEquals(task.id, deserialized.id)
        assertEquals(task.taskName, deserialized.taskName)
        assertEquals(task.state, deserialized.state)
    }
}