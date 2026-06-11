package io.celery.core

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class TaskConfigTest {

    @Test
    fun `default task config should have sensible defaults`() {
        val config = TaskConfig()

        assertFalse(config.allowConcurrentExecution)
        assertEquals(MisfirePolicy.IGNORE, config.misfirePolicy)
        assertEquals(0, config.maxRetries)
        assertEquals(1000L, config.retryDelayMs)
        assertEquals(30_000L, config.timeoutMs)
    }

    @Test
    fun `custom task config should accept valid values`() {
        val config = TaskConfig(
            allowConcurrentExecution = true,
            misfirePolicy = MisfirePolicy.FIRE_ALL,
            maxRetries = 5,
            retryDelayMs = 2000,
            timeoutMs = 60_000,
            maxRetryDelayMs = 120_000,
            retryBackoffMultiplier = 3.0
        )

        assertTrue(config.allowConcurrentExecution)
        assertEquals(MisfirePolicy.FIRE_ALL, config.misfirePolicy)
        assertEquals(5, config.maxRetries)
        assertEquals(2000L, config.retryDelayMs)
        assertEquals(60_000L, config.timeoutMs)
        assertEquals(120_000L, config.maxRetryDelayMs)
        assertEquals(3.0, config.retryBackoffMultiplier)
    }

    @Test
    fun `should throw on negative max retries`() {
        assertThrows(IllegalArgumentException::class.java) {
            TaskConfig(maxRetries = -1)
        }
    }

    @Test
    fun `should throw on zero retry delay`() {
        assertThrows(IllegalArgumentException::class.java) {
            TaskConfig(retryDelayMs = 0)
        }
    }

    @Test
    fun `should throw when max retry delay less than retry delay`() {
        assertThrows(IllegalArgumentException::class.java) {
            TaskConfig(retryDelayMs = 5000, maxRetryDelayMs = 3000)
        }
    }

    @Test
    fun `should throw on invalid backoff multiplier`() {
        assertThrows(IllegalArgumentException::class.java) {
            TaskConfig(retryBackoffMultiplier = 0.5)
        }
    }
}