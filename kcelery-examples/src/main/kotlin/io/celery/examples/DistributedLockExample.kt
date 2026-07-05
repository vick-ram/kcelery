package io.celery.examples

import io.celery.examples.config.ExampleConfig
import io.celery.lock.LockHandle
import io.celery.redis.RedisDistributedLock
import io.lettuce.core.RedisClient
import kotlinx.coroutines.*
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.milliseconds

/**
 * Distributed lock example showing:
 * - Lock acquisition with timeout
 * - Fencing tokens
 * - Lease renewal
 * - Concurrent access prevention
 * - Split-brain protection
 */
suspend fun main() {
    println("=" .repeat(60))
    println("kCelery Distributed Lock Example")
    println("=" .repeat(60))

    val redisUrl = ExampleConfig.getRedisUrl()
    val redisClient = RedisClient.create(redisUrl)
    val lock = RedisDistributedLock(redisClient)

    // Example 1: Basic lock usage
    println("\n1. Basic lock usage...")
    val result = lock.withLock(
        lockName = "resource:user:123",
        acquireTimeout = 5.seconds,
        leaseTtl = 30.seconds
    ) { handle: LockHandle ->
        println("   Lock acquired! Owner: ${handle.ownerId.take(8)}, Token: ${handle.fencingToken}")
        delay(500.milliseconds) // Simulate work
        println("   Work completed under lock")
        "success"
    }
    println("   Result: $result\n")

    // Example 2: Concurrent access prevention
    println("2. Concurrent access prevention...")
    val sharedCounter = java.util.concurrent.atomic.AtomicInteger(0)

    coroutineScope {
        // Launch 5 concurrent coroutines trying to access the same resource
        val jobs = (1..5).map { i ->
            launch {
                val lockResult = lock.withLock(
                    lockName = "counter-lock",
                    acquireTimeout = 10.seconds,
                    leaseTtl = 5.seconds
                ) { handle ->
                    println("   Coroutine $i acquired lock (token: ${handle.fencingToken})")
                    val current = sharedCounter.get()
                    delay(100.milliseconds) // Simulate work
                    sharedCounter.set(current + 1)
                    println("   Coroutine $i incremented counter to ${current + 1}")
                    current + 1
                }

                if (lockResult == null) {
                    println("   Coroutine $i failed to acquire lock")
                }
            }
        }

        jobs.joinAll()
    }

    println("   Final counter value: ${sharedCounter.get()}")
    println("   (Should be 5 - one increment per coroutine)\n")

    // Example 3: Fencing token for split-brain protection
    println("3. Fencing token demonstration...")

    val resourceState = java.util.concurrent.atomic.AtomicLong(0)

    // Simulate a scenario with fencing tokens
    coroutineScope {
        val job1 = launch {
            lock.withLock("fenced-resource", 5.seconds, 1.seconds) { handle ->
                println("   Job 1 acquired lock with token: ${handle.fencingToken}")
                resourceState.set(handle.fencingToken)
                println("   Job 1 wrote token ${handle.fencingToken} to resource")

                // Simulate long-running operation that exceeds TTL
                delay(1500.milliseconds) // Longer than 1 second TTL

                // Check if we still have the latest token
                if (resourceState.get() != handle.fencingToken) {
                    println("   ⚠️ Job 1 detected stale lock! Current token: ${resourceState.get()}")
                    return@withLock "stale-lock-detected"
                }

                println("   Job 1 completed with valid lock")
                "job1-success"
            }
        }

        // Job 2 acquires the lock after Job 1's TTL expires
        val job2 = launch {
            delay(1200.milliseconds) // Wait for Job 1's TTL to expire
            lock.withLock("fenced-resource", 5.seconds, 10.seconds) { handle ->
                println("   Job 2 acquired lock with token: ${handle.fencingToken}")
                resourceState.set(handle.fencingToken)
                println("   Job 2 wrote token ${handle.fencingToken} to resource")
                delay(300.milliseconds)
                println("   Job 2 completed")
                "job2-success"
            }
        }

        val result1 = job1.join()
        val result2 = job2.join()
        println("   Job 1 result: $result1")
        println("   Job 2 result: $result2\n")
    }

    // Example 4: Multiple independent locks
    println("4. Multiple independent locks...")

    coroutineScope {
        val start = System.currentTimeMillis()

        val lock1 = async {
            lock.withLock("lock-A", 5.seconds, 10.seconds) {
                println("   Lock A acquired")
                delay(500.milliseconds)
                println("   Lock A released")
                "A"
            }
        }

        val lock2 = async {
            lock.withLock("lock-B", 5.seconds, 10.seconds) {
                println("   Lock B acquired")
                delay(500.milliseconds)
                println("   Lock B released")
                "B"
            }
        }

        val lock3 = async {
            lock.withLock("lock-C", 5.seconds, 10.seconds) {
                println("   Lock C acquired")
                delay(500.milliseconds)
                println("   Lock C released")
                "C"
            }
        }

        val results = listOf(lock1.await(), lock2.await(), lock3.await())
        val elapsed = System.currentTimeMillis() - start

        println("   Results: $results")
        println("   Time: ${elapsed}ms")
        println("   (Should be ~500ms - locks acquired in parallel)\n")
    }

    // Example 5: Lock with timeout
    println("5. Lock acquisition timeout...")

    coroutineScope {
        // Hold a lock for a long time
        val holder = launch {
            lock.withLock("timeout-demo", 1.seconds, 5.seconds) {
                println("   Lock holder acquired lock")
                delay(3.seconds) // Hold for 3 seconds
                println("   Lock holder releasing")
            }
        }

        // Try to acquire with short timeout
        delay(100.milliseconds) // Let holder acquire first
        val timedOut = lock.withLock(
            lockName = "timeout-demo",
            acquireTimeout = 500.milliseconds, // Only wait 500ms
            leaseTtl = 10.seconds
        ) {
            println("   This should not print")
        }

        println("   Timeout result: ${if (timedOut == null) "Correctly timed out" else "Unexpectedly acquired"}")

        holder.join()
    }

    // Cleanup
    lock.close()
    redisClient.shutdown()
    ExampleConfig.shutdown()
    println("\n✅ Distributed lock example completed!")
}