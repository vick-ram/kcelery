package io.celery.core

import io.celery.config.InstantSerializer
import kotlinx.serialization.Serializable
import java.time.Instant

@Serializable
sealed interface Trigger {
    fun nextExecutionTime(clock: Clock, lastExecution: Instant? = null): Instant?

    @Serializable
    data class CronTrigger(val expression: CronExpression) : Trigger {
        override fun nextExecutionTime(clock: Clock, lastExecution: Instant?): Instant {
            val now = clock.instant()
            val baseTime = lastExecution?.let {
                if (it.isAfter(now)) now else it
            } ?: now

            val zonedNow = java.time.ZonedDateTime.ofInstant(baseTime, clock.zone())
            return expression.nextMatchAfter(zonedNow).toInstant()
        }
    }

    @Serializable
    data class FixedDelayTrigger(
        val delayMs: Long
    ) : Trigger {
        override fun nextExecutionTime(clock: Clock, lastExecution: Instant?): Instant {
            val base = lastExecution ?: clock.instant()
            return base.plusMillis(delayMs)
        }
    }

    @Serializable
    data class FixedRateTrigger(
        val periodMs: Long,
        @Serializable(with = InstantSerializer::class)
        val startTime: Instant
    ) : Trigger {
        override fun nextExecutionTime(clock: Clock, lastExecution: Instant?): Instant {
            val now = clock.instant()
            if (now.isBefore(startTime)) return startTime

            val elapsed = now.toEpochMilli() - startTime.toEpochMilli()
            val periods = elapsed / periodMs + 1
            return startTime.plusMillis(periods * periodMs)
        }
    }
}