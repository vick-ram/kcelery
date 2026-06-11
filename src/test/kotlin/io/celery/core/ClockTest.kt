package io.celery.core

import io.celery.test.TestBase
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.time.Instant
import java.time.ZoneOffset

@OptIn(ExperimentalCoroutinesApi::class)
class ClockTest : TestBase() {

    @Test
    fun `system clock should return current time`() {
        val systemClock = Clock.systemDefault()
        val before = Instant.now()
        val clockTime = systemClock.instant()
        val after = Instant.now()

        Assertions.assertTrue(clockTime >= before)
        Assertions.assertTrue(clockTime <= after)
    }

    @Test
    fun `utc clock should use UTC timezone`() {
        val utcClock = Clock.utc()
        Assertions.assertEquals(ZoneOffset.UTC, utcClock.zone())
    }

    @Test
    fun `fixed clock should return fixed time`() {
        val fixedTime = Instant.parse("2024-01-15T10:00:00Z")
        val fixedClock = Clock.fixed(fixedTime)

        Assertions.assertEquals(fixedTime, fixedClock.instant())
        Assertions.assertEquals(fixedTime, fixedClock.instant()) // Should be idempotent
    }

    @Test
    fun `fixed clock with custom timezone`() {
        val fixedTime = Instant.parse("2024-01-15T10:00:00Z")
        val zone = ZoneOffset.ofHours(3)
        val fixedClock = Clock.fixed(fixedTime, zone)

        Assertions.assertEquals(fixedTime, fixedClock.instant())
        Assertions.assertEquals(zone, fixedClock.zone())
    }

    @Test
    fun `millis should convert correctly`() {
        val clock = Clock.fixed(Instant.parse("2024-01-15T10:00:00Z"))
        Assertions.assertEquals(1705312800000L, clock.millis())
    }
}