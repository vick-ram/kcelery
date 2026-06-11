package io.celery.redis

import java.util.concurrent.ConcurrentHashMap
import kotlinx.coroutines.*
import kotlinx.coroutines.future.await
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
    private val renewalJobs = ConcurrentHashMap<String, Job>()

    // Dedicated dispatcher for blocking Redis operations
    private val redisDispatcher = Dispatchers.IO.limitedParallelism(10)

    override suspend fun <T> withLock(
        lockName: String,
        timeout: Duration,
        block: suspend () -> T
    ): T? {
        val lock = redissonClient.getLock("celery:lock:$lockName")

        return try {
            val acquired = withTimeout(timeout) {
                withContext(redisDispatcher) {
                    lock.tryLock(timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)
                }
            }

            if (acquired) {
                try {
                    block()
                } finally {
                    withContext(redisDispatcher) {
                        try {
                            if (lock.isHeldByCurrentThread) {
                                lock.unlock()
                            }
                        } catch (e: Exception) {
                            logger.error("Failed to unlock: $lockName", e)
                        }
                    }
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
            val acquired = withContext(redisDispatcher) {
                lock.tryLock(ttl.inWholeMilliseconds, TimeUnit.MILLISECONDS)
            }

            if (acquired) {
                leaderHolders.add(leaderKey)
                renewalJobs[leaderKey]?.cancel()

                val job = CoroutineScope(redisDispatcher + SupervisorJob()).launch {
                    try {
                        while (isActive && leaderHolders.contains(leaderKey)) {
                            delay((ttl / 2).inWholeMilliseconds)
                            lock.tryLock(ttl.inWholeMilliseconds, TimeUnit.MILLISECONDS)
                        }
                    } catch (e: Exception) {
                        logger.error("Failed to renew leadership: $leaderKey", e)
                    } finally {
                        leaderHolders.remove(leaderKey)
                        renewalJobs.remove(leaderKey)
                        if (lock.isHeldByCurrentThread) {
                            lock.unlock()
                        }
                    }
                }

                renewalJobs[leaderKey] = job
            }
            acquired
        } catch (e: Exception) {
            logger.error("Failed to acquire leadership: $leaderKey", e)
            false
        }
    }
}