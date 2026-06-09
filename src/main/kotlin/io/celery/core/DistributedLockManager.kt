package io.celery.core

import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.*
import org.redisson.api.RedissonClient
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

interface DistributedLockManager {
    suspend fun <T> withLock(lockName: String, timeout: Duration = 30.seconds, block: suspend () -> T): T?
    suspend fun tryAcquireLeadership(leaderKey: String, ttl: Duration = 30.seconds): Boolean
}

class RedisDistributedLockManager(private val redissonClient: RedissonClient) : DistributedLockManager {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val leaderHolders = ConcurrentHashMap.newKeySet<String>()

    override suspend fun <T> withLock(
        lockName: String,
        timeout: Duration,
        block: suspend () -> T
    ): T? {
        val lock = redissonClient.getLock("celery:lock:$lockName")

        return try {
            val acquired = withTimeout(timeout) {
                lock.tryLock(0, timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
            }

            if (acquired) {
                try {
                    block()
                } finally {
                    lock.unlock()
                }
            } else {
                logger.warn("Failed to acquire lock: $lockName within timeout")
                null
            }
        } catch (_: TimeoutCancellationException) {
            logger.warn("Lock acquisition timed out: $lockName")
            null
        } catch (e: Exception) {
            logger.error("Error during locked operation: $lockName", e)
            throw e
        }
    }

    override suspend fun tryAcquireLeadership(leaderKey: String, ttl: Duration): Boolean {
        val lock = redissonClient.getLock("celery:leader:$leaderKey")
        return try {
            val acquired = lock.tryLock(0, ttl.inWholeMilliseconds, TimeUnit.MILLISECONDS)
            if (acquired) {
                leaderHolders.add(leaderKey)
                // Start renewal coroutine
                CoroutineScope(Dispatchers.Default).launch {
                    try {
                        while (isActive && leaderHolders.contains(leaderKey)) {
                            delay((ttl / 2).inWholeMilliseconds.milliseconds)
                            lock.tryLock(0, ttl.inWholeMilliseconds, TimeUnit.MILLISECONDS)
                        }
                    } finally {
                        leaderHolders.remove(leaderKey)
                    }
                }
            }
            acquired
        } catch (e: Exception) {
            logger.error("Failed to acquire leadership: $leaderKey", e)
            false
        }
    }
}