package io.celery.trigger

import io.celery.test.TestBase
import io.celery.trigger.CronExpression
import kotlinx.coroutines.ExperimentalCoroutinesApi
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import java.time.DayOfWeek
import java.time.ZonedDateTime

@OptIn(ExperimentalCoroutinesApi::class)
class CronExpressionTest : TestBase() {

    @Test
    fun `parse 5-field cron expression`() {
        val expr = CronExpression.parse("0 12 * * MON-FRI")

        assertEquals(setOf(0), expr.seconds)
        assertEquals(setOf(12), expr.minutes)
        assertEquals(setOf(0), expr.hours)
        assertEquals((1..31).toSet(), expr.daysOfMonth) // '*' expands to all
        assertEquals((1..12).toSet(), expr.months)
        assertTrue(expr.daysOfWeek.containsAll(listOf(DayOfWeek.MONDAY, DayOfWeek.FRIDAY)))
    }

    @Test
    fun `parse 6-field cron expression`() {
        val expr = CronExpression.parse("*/15 0 1,13 * * 1-5")

        assertEquals(setOf(0, 15, 30, 45), expr.seconds)
        assertEquals(setOf(0), expr.minutes)
        assertEquals(setOf(1, 13), expr.hours)
    }

    @ParameterizedTest
    @CsvSource(
        "0 0 12 * * *, 2024-01-15T12:00:00Z, true",
        "0 0 12 * * *, 2024-01-15T12:00:01Z, false",
        "0 30 9 * * 1-5, 2024-01-15T09:30:00Z, true", // Monday
        "0 30 9 * * 1-5, 2024-01-14T09:30:00Z, false" // Sunday
    )
    fun `should match cron expression correctly`(
        expression: String,
        dateTime: String,
        expected: Boolean
    ) {
        val expr = CronExpression.parse(expression)
        val zdt = ZonedDateTime.parse(dateTime)

        assertEquals(expected, expr.matches(zdt))
    }

    @Test
    fun `find next match for simple cron`() {
        val expr = CronExpression.parse("0 0 12 * * *") // Every day at noon
        val now = ZonedDateTime.parse("2024-01-15T10:00:00Z")

        val next = expr.nextMatchAfter(now)

        assertEquals(ZonedDateTime.parse("2024-01-15T12:00:00Z"), next)
    }

    @Test
    fun `find next match for complex cron`() {
        val expr = CronExpression.parse("0 30 9 * * 1-5") // 9:30 AM weekdays
        val now = ZonedDateTime.parse("2024-01-12T09:30:00Z") // Friday

        val next = expr.nextMatchAfter(now)

        // Should be next Monday
        assertEquals(DayOfWeek.MONDAY, next.dayOfWeek)
        assertEquals(9, next.hour)
        assertEquals(30, next.minute)
    }

    @Test
    fun `find next match for step values`() {
        val expr = CronExpression.parse("0 */15 * * * *") // Every 15 minutes
        val now = ZonedDateTime.parse("2024-01-15T10:05:00Z")

        val next = expr.nextMatchAfter(now)

        assertEquals(ZonedDateTime.parse("2024-01-15T10:15:00Z"), next)
    }

    @Test
    fun `find next match for month boundaries`() {
        val expr = CronExpression.parse("0 0 0 1 * *") // First day of month
        val now = ZonedDateTime.parse("2024-01-15T10:00:00Z")

        val next = expr.nextMatchAfter(now)

        assertEquals(ZonedDateTime.parse("2024-02-01T00:00:00Z"), next)
    }

    @Test
    fun `find next match for year boundaries`() {
        val expr = CronExpression.parse("0 0 0 1 1 *") // Jan 1st
        val now = ZonedDateTime.parse("2024-01-01T00:00:00Z")

        val next = expr.nextMatchAfter(now)

        assertEquals(2025, next.year)
        assertEquals(1, next.monthValue)
        assertEquals(1, next.dayOfMonth)
    }

    @Test
    fun `should throw on invalid expression`() {
        assertThrows(IllegalArgumentException::class.java) {
            CronExpression.parse("invalid expression")
        }
    }

    @Test
    fun `should throw on too many fields`() {
        assertThrows(IllegalArgumentException::class.java) {
            CronExpression.parse("0 0 0 0 0 0 0")
        }
    }

    @Test
    fun `parse with L character`() {
        val expr = CronExpression.parse("0 0 0 L * *")
        assertEquals(setOf(31), expr.daysOfMonth)
    }

    @ParameterizedTest
    @ValueSource(strings = [
        "SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT",
        "0", "1", "2", "3", "4", "5", "6", "7"
    ])
    fun `parse day of week variations`(day: String) {
        val expr = CronExpression.parse("0 0 0 * * $day")
        assertTrue(expr.daysOfWeek.isNotEmpty())
    }

    @Test
    fun `complex cron with ranges and steps`() {
        val expr = CronExpression.parse("0 0-30/10 9-17 * * MON-FRI")

        assertEquals(setOf(0), expr.seconds)
        assertEquals(setOf(0, 10, 20, 30), expr.minutes)
        assertEquals((9..17).toSet(), expr.hours)
        assertTrue(expr.daysOfWeek.containsAll(
            listOf(DayOfWeek.MONDAY, DayOfWeek.TUESDAY,
                DayOfWeek.WEDNESDAY, DayOfWeek.THURSDAY, DayOfWeek.FRIDAY)
        ))
    }
}