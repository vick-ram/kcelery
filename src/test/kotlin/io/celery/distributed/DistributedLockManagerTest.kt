package io.celery.distributed

import io.celery.redis.RedisDistributedLockManager
import io.celery.test.RedisTestBase
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class DistributedLockManagerTest : RedisTestBase() {

    @Test
    fun `should acquire and release lock`() = runTest {
        val result = lockManager.withLock("test-lock") {
            "success"
        }

        Assertions.assertEquals("success", result)
    }

    @Test
    fun `should prevent concurrent access to same lock`() = runTest {
        val executionOrder = mutableListOf<Int>()

        coroutineScope {
            launch {
                lockManager.withLock("concurrent-lock") {
                    executionOrder.add(1)
                    delay(500.milliseconds)
                    executionOrder.add(2)
                }
            }

            launch {
                delay(100.milliseconds) // Ensure first coroutine acquires lock first
                lockManager.withLock("concurrent-lock") {
                    executionOrder.add(3)
                }
            }
        }

        Assertions.assertEquals(listOf(1, 2, 3), executionOrder)
    }

    @Test
    fun `should timeout when lock cannot be acquired`() = runTest {
        val lockAcquired = AtomicBoolean(false)

        coroutineScope {
            launch {
                lockManager.withLock("timeout-lock") {
                    lockAcquired.set(true)
                    delay(5000.milliseconds) // Hold lock for long time
                }
            }

            delay(200.milliseconds) // Wait for first coroutine to acquire lock

            val result = lockManager.withLock(
                lockName = "timeout-lock",
                timeout = 1.seconds
            ) {
                "should not execute"
            }

            Assertions.assertNull(result, "Should timeout and return null")
        }
    }

    @Test
    fun `should successfully acquire leadership`() = runTest {
        val isLeader =
            lockManager.tryAcquireLeadership("test-leader", 30.seconds)
        assertTrue(isLeader, "First instance should become leader")
    }

    @Test
    fun `second instance should not become leader`() = runTest {
        // First instance becomes leader
        val isLeader1 = lockManager.tryAcquireLeadership(
            "test-leader-2",
            30.seconds
        )
        assertTrue(isLeader1)

        // Create second lock manager
        val secondLockManager =
            RedisDistributedLockManager(redissonClient)

        // Second instance should not become leader
        val isLeader2 = secondLockManager.tryAcquireLeadership("test-leader-2", 30.seconds)
        Assertions.assertFalse(isLeader2, "Second instance should not become leader")
    }
}