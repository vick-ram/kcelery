package io.celery.core

import java.time.*

interface Clock {
    fun instant(): Instant
    fun zone(): ZoneId

    fun millis(): Long = instant().toEpochMilli()

    companion object {
        fun systemDefault(): Clock = SystemClock(ZoneId.systemDefault())
        fun utc(): Clock = SystemClock(ZoneOffset.UTC)
        fun fixed(instant: Instant, zone: ZoneId = ZoneOffset.UTC): Clock = FixedClock(instant, zone)
    }
}

internal class SystemClock(private val zoneId: ZoneId) : Clock {
    override fun instant(): Instant = Instant.now()
    override fun zone(): ZoneId = zoneId
}

internal class FixedClock(private val fixedInstant: Instant, private val zoneId: ZoneId) : Clock {
    override fun instant(): Instant = fixedInstant
    override fun zone(): ZoneId = zoneId
}