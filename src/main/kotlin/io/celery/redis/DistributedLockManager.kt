package io.celery.redis

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

interface DistributedLockManager {
    suspend fun <T> withLock(lockName: String, timeout: Duration = 30.seconds, block: suspend () -> T): T?
    suspend fun tryAcquireLeadership(leaderKey: String, ttl: Duration = 30.seconds): Boolean
}

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class RedisDistributedLockManager(redisClient: RedisClient) : DistributedLockManager, AutoCloseable {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val connection: StatefulRedisConnection<String, String> = redisClient.connect()
    private val redisCommands = connection.coroutines()

    private val leaderHolders = ConcurrentHashMap.newKeySet<String>()
    private val renewalJobs = ConcurrentHashMap<String, Job>()
    private val lockIdGenerator = AtomicLong(1)
    private val managerScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val acquireLockScript = """
        if redis.call('exists', KEYS[1]) == 0 then
            redis.call('hset', KEYS[1], ARGV[1], 1)
            redis.call('pexpire', KEYS[1], ARGV[2])
            return 1
        end
        return 0
    """.trimIndent()

    private val renewLockScript = """
        if redis.call('hexists', KEYS[1], ARGV[1]) == 1 then
            redis.call('pexpire', KEYS[1], ARGV[2])
            return 1
        end
        return 0
    """.trimIndent()

    private val releaseLockScript = """
        if redis.call('hexists', KEYS[1], ARGV[1]) == 1 then
            redis.call('del', KEYS[1])
            return 1
        end
        return 0
    """.trimIndent()

    override suspend fun <T> withLock(
        lockName: String,
        timeout: Duration,
        block: suspend () -> T
    ): T? {
        val lockId = "${lockIdGenerator.getAndIncrement()}-${System.nanoTime()}"
        val lockKey = "celery:lock:$lockName"
        var isLockAcquired = false

        return try {
            // Fix #2: Use NonCancellable block when dealing with strict atomic state assignment
            withTimeout(timeout) {
                val deadline = System.currentTimeMillis() + timeout.inWholeMilliseconds
                while (!isLockAcquired && System.currentTimeMillis() < deadline) {
                    ensureActive() // Cooperatively check for cancellation before hitting Redis
                    try {
                        val result = redisCommands.eval<Long>(
                            acquireLockScript,
                            ScriptOutputType.INTEGER,
                            arrayOf(lockKey),
                            lockId,
                            timeout.inWholeMilliseconds.toString() // Fix #1: Dynamic TTL matching timeout
                        )
                        if (result == 1L) {
                            isLockAcquired = true
                        } else {
                            delay(100.milliseconds)
                        }
                    } catch (e: Exception) {
                        logger.error("Error communicating with Redis while acquiring: $lockName", e)
                        delay(100.milliseconds)
                    }
                }
            }

            if (isLockAcquired) {
                block()
            } else {
                null
            }
        } catch (_: TimeoutCancellationException) {
            logger.warn("Lock acquisition timed out or cancelled for: $lockName")
            null
        } finally {
            // Fix #2: If we acquired it but a cancellation hit right after, we MUST release it safely
            if (isLockAcquired) {
                // Run in NonCancellable context to ensure the release script actually hits Redis
                withContext(NonCancellable) {
                    try {
                        redisCommands.eval<Long>(
                            releaseLockScript,
                            ScriptOutputType.INTEGER,
                            arrayOf(lockKey),
                            lockId
                        )
                    } catch (e: Exception) {
                        logger.error("Failed to release lock cleanly: $lockName", e)
                    }
                }
            }
        }
    }

    override suspend fun tryAcquireLeadership(leaderKey: String, ttl: Duration): Boolean {
        val leaderId = "${lockIdGenerator.getAndIncrement()}-${System.nanoTime()}"
        val lockKey = "celery:leader:$leaderKey"

        val acquired = try {
            val result = redisCommands.eval<Long>(
                acquireLockScript,
                ScriptOutputType.INTEGER,
                arrayOf(lockKey),
                leaderId,
                ttl.inWholeMilliseconds.toString()
            )
            result == 1L
        } catch (e: Exception) {
            logger.error("Error acquiring leadership: $leaderKey", e)
            false
        }

        if (acquired) {
            leaderHolders.add(leaderKey)
            renewalJobs[leaderKey]?.cancel()

            val job = managerScope.launch {
                try {
                    while (isActive && leaderHolders.contains(leaderKey)) {
                        delay((ttl / 3).inWholeMilliseconds.milliseconds)

                        try {
                            val renewed = redisCommands.eval<Long>(
                                renewLockScript,
                                ScriptOutputType.INTEGER,
                                arrayOf(lockKey),
                                leaderId,
                                ttl.inWholeMilliseconds.toString()
                            )

                            if (renewed != 1L) {
                                logger.warn("Lost leadership lease in Redis for: $leaderKey")
                                break
                            }
                        } catch (e: CancellationException) {
                            throw e
                        } catch (e: Exception) {
                            logger.error("Network error renewing leadership: $leaderKey", e)
                            // Don't break immediately on single transient network failure; try again next loop
                        }
                    }
                } finally {
                    // Ensure local state cleanup happens atomically
                    leaderHolders.remove(leaderKey)
                    renewalJobs.remove(leaderKey)

                    withContext(NonCancellable) {
                        try {
                            redisCommands.eval<Long>(
                                releaseLockScript,
                                ScriptOutputType.INTEGER,
                                arrayOf(lockKey),
                                leaderId
                            )
                        } catch (e: Exception) {
                            logger.error("Error releasing leadership key from Redis: $leaderKey", e)
                        }
                    }
                }
            }
            renewalJobs[leaderKey] = job
        }
        return acquired
    }

    override fun close() {
        managerScope.cancel()
        renewalJobs.clear()
        leaderHolders.clear()
        connection.close() // Close the connection lifecycle safely
    }

}
