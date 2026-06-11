package io.celery.trigger

import io.celery.test.TestBase
import io.celery.trigger.CronExpression
import io.celery.trigger.Trigger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.time.Instant

@OptIn(ExperimentalCoroutinesApi::class)
class TriggerTest : TestBase() {

    @Test
    fun `cron trigger should calculate next execution`() {
        val trigger = Trigger.CronTrigger(
            CronExpression.parse("0 0 12 * * *")
        )

        val next = trigger.nextExecutionTime(clock)

        assertEquals(Instant.parse("2024-01-15T12:00:00Z"), next)
    }

    @Test
    fun `fixed delay trigger should use last execution time`() {
        val lastExec = Instant.parse("2024-01-15T09:00:00Z")
        val trigger = Trigger.FixedDelayTrigger(
            delayMs = 300_000, // 5 minutes
        )

        val next = trigger.nextExecutionTime(clock)

        assertEquals(Instant.parse("2024-01-15T09:05:00Z"), next)
    }

    @Test
    fun `fixed delay trigger without last execution should use current time`() {
        val trigger = Trigger.FixedDelayTrigger(
            delayMs = 300_000 // 5 minutes
        )

        val next = trigger.nextExecutionTime(clock)

        assertEquals(clock.instant().plusMillis(300_000), next)
    }

    @Test
    fun `fixed rate trigger should calculate based on start time`() {
        val startTime = Instant.parse("2024-01-15T08:00:00Z")
        val trigger = Trigger.FixedRateTrigger(
            periodMs = 300_000, // 5 minutes
            startTime = startTime
        )

        val next = trigger.nextExecutionTime(clock)

        // Clock is at 10:00, so next should be after that
        assertTrue(next.isAfter(clock.instant()))
        // Should be a multiple of period from start
        val elapsed = next.toEpochMilli() - startTime.toEpochMilli()
        assertEquals(0, elapsed % 300_000)
    }

    @Test
    fun `fixed rate trigger before start time should return start time`() {
        val startTime = Instant.parse("2024-01-15T12:00:00Z")
        val trigger = Trigger.FixedRateTrigger(
            periodMs = 300_000,
            startTime = startTime
        )

        // Clock is at 10:00, before start time
        val next = trigger.nextExecutionTime(clock)

        assertEquals(startTime, next)
    }

    @Test
    fun `serialization of triggers`() {
        val cronTrigger = Trigger.CronTrigger(
            CronExpression.parse("0 0 * * * *")
        )
        val json = kotlinx.serialization.json.Json.encodeToString(
            Trigger.serializer(), cronTrigger
        )

        assertTrue(json.contains("CronTrigger"))
        assertTrue(json.contains("0 0 * * * *"))
    }
}