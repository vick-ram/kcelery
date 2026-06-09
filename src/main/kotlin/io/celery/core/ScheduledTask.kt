package io.celery.core

import io.celery.TaskState
import io.celery.config.InstantSerializer
import kotlinx.serialization.Serializable
import java.time.Instant
import java.util.concurrent.Delayed
import java.util.concurrent.TimeUnit

@Serializable
data class ScheduledTask(
    val id: String,
    val trigger: Trigger,
    val config: TaskConfig,
    val taskName: String,
    val version: Long = 0,
    @Serializable(with = InstantSerializer::class)
    val nextExecutionTime: Instant? = null,
    @Serializable(with = InstantSerializer::class)
    val lastExecutionTime: Instant? = null,
    val state: TaskState = TaskState.SCHEDULED,
    @Serializable(with = InstantSerializer::class)
    val createdAt: Instant = Instant.now(),
    @Serializable(with = InstantSerializer::class)
    val updatedAt: Instant = Instant.now()
) : Delayed {

    override fun getDelay(unit: TimeUnit): Long {
        return nextExecutionTime?.let { next ->
            val delayMs = next.toEpochMilli() - System.currentTimeMillis()
            unit.convert(maxOf(0, delayMs), TimeUnit.MILLISECONDS)
        } ?: Long.MAX_VALUE
    }

    override fun compareTo(other: Delayed): Int {
        if (other !is ScheduledTask) return 1

        val thisTime = this.nextExecutionTime ?: Instant.MAX
        val otherTime = other.nextExecutionTime ?: Instant.MAX
        return thisTime.compareTo(otherTime)
    }

    fun withNextExecution(clock: Clock, lastExecution: Instant? = null): ScheduledTask {
        val nextTime = trigger.nextExecutionTime(clock, lastExecution)
        return copy(
            nextExecutionTime = nextTime,
            version = version + 1,
            updatedAt = clock.instant()
        )
    }
}