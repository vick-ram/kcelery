package io.celery.redis

import io.celery.lock.*
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.ScriptOutputType
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Pure Redis/Lettuce-based distributed lock implementation.
 * 
 * Uses Redis Lua scripts for atomic operations and fencing tokens
 * to prevent split-brain scenarios. Includes automatic lease renewal
 * via watchdog coroutines.
 * 
 * Key features:
 * - Fencing tokens for split-brain protection
 * - Automatic lease renewal (watchdog)
 * - Compare-and-delete for safe lock release
 * - Coroutine-friendly with proper cancellation handling
 */
@OptIn(ExperimentalLettuceCoroutinesApi::class)
class RedisDistributedLock(
    private val redisClient: RedisClient,
    private val config: RedisLockConfig = RedisLockConfig()
) : DistributedLock {

    private val logger = LoggerFactory.getLogger(javaClass)

    // Dedicated connection for lock operations
    private val connection: StatefulRedisConnection<String, String> = redisClient.connect()
    private val redisCommands = connection.coroutines()

    // Track held locks: lockName -> ownerId
    private val heldLocks = ConcurrentHashMap<String, String>()

    // Watchdog renewal jobs: lockName -> Job
    private val renewalJobs = ConcurrentHashMap<String, Job>()

    // Supervisor scope for watchdog coroutines
    private val managerScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    // Fencing token cache: fenceKey -> current token
    private val fenceTokens = ConcurrentHashMap<String, Long>()

    /**
     * Lua script for atomic lock acquisition.
     * 
     * KEYS[1] = lock key
     * ARGV[1] = owner ID
     * ARGV[2] = TTL in milliseconds
     * 
     * Returns 1 if lock acquired, 0 if already held.
     */
    private val acquireScript = """
        if redis.call('exists', KEYS[1]) == 0 then
            redis.call('hset', KEYS[1], 'owner', ARGV[1], 'fence', '0')
            redis.call('pexpire', KEYS[1], ARGV[2])
            return 1
        end
        return 0
    """.trimIndent()

    /**
     * Lua script for atomic lock renewal.
     * Only renews if we still own the lock (compare-and-renew).
     * 
     * KEYS[1] = lock key
     * ARGV[1] = owner ID
     * ARGV[2] = TTL in milliseconds
     * 
     * Returns 1 if renewed, 0 if we don't own the lock.
     */
    private val renewScript = """
        if redis.call('hget', KEYS[1], 'owner') == ARGV[1] then
            redis.call('pexpire', KEYS[1], ARGV[2])
            return 1
        end
        return 0
    """.trimIndent()

    /**
     * Lua script for atomic lock release.
     * Only deletes if we still own the lock (compare-and-delete).
     * 
     * KEYS[1] = lock key
     * ARGV[1] = owner ID
     * 
     * Returns 1 if released, 0 if we don't own the lock.
     */
    private val releaseScript = """
        if redis.call('hget', KEYS[1], 'owner') == ARGV[1] then
            redis.call('del', KEYS[1])
            return 1
        end
        return 0
    """.trimIndent()

    /**
     * Lua script for monotonically increasing fencing token.
     * 
     * KEYS[1] = fence key
     * 
     * Returns the new (incremented) fencing token.
     */
    private val fenceScript = """
        local token = redis.call('incr', KEYS[1])
        redis.call('hset', KEYS[1] .. ':lock', 'fence', token)
        return token
    """.trimIndent()

    /**
     * Execute a block of code under a distributed lock with fencing token protection.
     */
    override suspend fun <T> withLock(
        lockName: String,
        acquireTimeout: Duration,
        leaseTtl: Duration,
        block: suspend (LockHandle) -> T
    ): T? {
        val ownerId = UUID.randomUUID().toString()
        val lockKey = lockKey(lockName)
        var acquired = false

        val result: T? = try {
            // Wait for lock acquisition with timeout
            withTimeout(acquireTimeout) {
                while (!acquired) {
                    ensureActive()
                    acquired = tryAcquireRaw(lockKey, ownerId, leaseTtl)
                    if (!acquired) {
                        delay(100.milliseconds)
                    }
                }
            }

            if (!acquired) {
                logger.warn("Failed to acquire lock '$lockName' within $acquireTimeout")
                return null
            }

            // Get fencing token
            val fencingToken = nextFencingToken(lockName)
            val handle = LockHandle(lockName, ownerId, fencingToken)

            // Track the lock
            heldLocks[lockName] = ownerId

            // Start watchdog for automatic lease renewal
            val renewalJob = startWatchdog(lockKey, lockName, ownerId, leaseTtl)
            renewalJobs[lockName] = renewalJob

            try {
                logger.debug("Executing block under lock '$lockName' (token: $fencingToken)")
                block(handle)
            } finally {
                // Clean up
                renewalJob.cancel()
                renewalJobs.remove(lockName)
                heldLocks.remove(lockName)

                // Release lock in non-cancellable context
                withContext(NonCancellable) {
                    releaseRaw(lockKey, ownerId)
                }
                logger.debug("Released lock '{}'", lockName)
            }
        } catch (_: TimeoutCancellationException) {
            logger.warn("Lock acquisition timed out for '$lockName' after $acquireTimeout")
            null
        } catch (e: CancellationException) {
            logger.debug("Lock operation cancelled for '$lockName'")
            throw e
        } catch (e: Exception) {
            logger.error("Error during locked operation '$lockName'", e)
            throw e
        }

        return result
    }

    /**
     * Try to acquire leadership for a group.
     */
    override suspend fun tryAcquireLeadership(
        leaderKey: String,
        ttl: Duration
    ): LockHandle? {
        val ownerId = UUID.randomUUID().toString()
        val lockKey = leaderKey(leaderKey)

        return try {
            val acquired = tryAcquireRaw(lockKey, ownerId, ttl)

            if (!acquired) {
                logger.debug("Failed to acquire leadership for '$leaderKey'")
                return null
            }

            val fencingToken = nextFencingToken("leader:$leaderKey")
            val handle = LockHandle(leaderKey, ownerId, fencingToken)

            heldLocks[leaderKey] = ownerId

            // Start watchdog for leadership renewal
            val renewalJob = startWatchdog(lockKey, leaderKey, ownerId, ttl)
            renewalJobs[leaderKey] = renewalJob

            logger.info("Acquired leadership for '$leaderKey' (token: $fencingToken)")
            handle

        } catch (e: Exception) {
            logger.error("Error acquiring leadership '$leaderKey'", e)
            null
        }
    }

    /**
     * Release a lock or leadership.
     */
    override suspend fun release(handle: LockHandle) {
        // Cancel watchdog
        renewalJobs[handle.lockName]?.cancel()
        renewalJobs.remove(handle.lockName)
        heldLocks.remove(handle.lockName)

        val key = if (handle.lockName.startsWith("leader:")) {
            leaderKey(handle.lockName.removePrefix("leader:"))
        } else {
            lockKey(handle.lockName)
        }

        withContext(NonCancellable) {
            releaseRaw(key, handle.ownerId)
        }

        logger.debug("Released lock '{}'", handle.lockName)
    }

    /**
     * Check if lock is currently held.
     */
    override suspend fun isLocked(lockName: String): Boolean {
        return try {
            redisCommands.exists(lockKey(lockName))!! > 0 // @TODO
        } catch (e: Exception) {
            logger.error("Failed to check lock status: $lockName", e)
            false
        }
    }

    /**
     * Get the current lock owner.
     */
    override suspend fun getLockOwner(lockName: String): String? {
        return try {
            redisCommands.hget(lockKey(lockName), "owner")
        } catch (e: Exception) {
            logger.error("Failed to get lock owner: $lockName", e)
            null
        }
    }

    /**
     * Get current fencing token.
     */
    override suspend fun getFencingToken(lockName: String): Long {
        return try {
            val token = redisCommands.hget(lockKey(lockName), "fence")
            token?.toLongOrNull() ?: 0L
        } catch (e: Exception) {
            logger.error("Failed to get fencing token: $lockName", e)
            0L
        }
    }

    /**
     * Close the lock manager.
     */
    override fun close() {
        // Cancel all watchdogs
        renewalJobs.values.forEach { it.cancel() }
        renewalJobs.clear()
        heldLocks.clear()

        managerScope.cancel()
        connection.close()

        logger.info("RedisDistributedLock closed")
    }

    // --- Private helpers ---

    /**
     * Try to acquire the lock atomically.
     */
    private suspend fun tryAcquireRaw(
        lockKey: String,
        ownerId: String,
        ttl: Duration
    ): Boolean {
        return try {
            val result = redisCommands.eval<Long>(
                acquireScript,
                ScriptOutputType.INTEGER,
                arrayOf(lockKey),
                ownerId,
                ttl.inWholeMilliseconds.toString()
            )
            result == 1L
        } catch (e: Exception) {
            logger.error("Failed to acquire lock: $lockKey", e)
            false
        }
    }

    /**
     * Try to renew the lock atomically.
     */
    private suspend fun renewRaw(
        lockKey: String,
        ownerId: String,
        ttl: Duration
    ): Boolean {
        return try {
            val result = redisCommands.eval<Long>(
                renewScript,
                ScriptOutputType.INTEGER,
                arrayOf(lockKey),
                ownerId,
                ttl.inWholeMilliseconds.toString()
            )
            result == 1L
        } catch (e: Exception) {
            logger.error("Failed to renew lock: $lockKey", e)
            false
        }
    }

    /**
     * Release the lock atomically.
     */
    private suspend fun releaseRaw(lockKey: String, ownerId: String): Boolean {
        return try {
            val result = redisCommands.eval<Long>(
                releaseScript,
                ScriptOutputType.INTEGER,
                arrayOf(lockKey),
                ownerId
            )
            result == 1L
        } catch (e: Exception) {
            logger.error("Failed to release lock: $lockKey", e)
            false
        }
    }

    /**
     * Get the next fencing token for a lock.
     */
    private suspend fun nextFencingToken(name: String): Long {
        val fenceKey = "celery:fence:$name"
        return try {
            val token = redisCommands.eval<Long>(
                fenceScript,
                ScriptOutputType.INTEGER,
                arrayOf(fenceKey)
            ) ?: error("Fencing token script returned null for $fenceKey")

            fenceTokens[name] = token
            token
        } catch (e: Exception) {
            logger.error("Failed to get fencing token for: $name", e)
            throw LockException("Failed to get fencing token", e)
        }
    }

    /**
     * Start watchdog coroutine for automatic lock renewal.
     */
    private fun startWatchdog(
        lockKey: String,
        lockName: String,
        ownerId: String,
        ttl: Duration
    ): Job {
        return managerScope.launch {
            try {
                // Renew at 1/3 of TTL to ensure we never expire
                val renewalInterval = (ttl / 3).inWholeMilliseconds.milliseconds

                while (isActive && heldLocks[lockName] == ownerId) {
                    delay(renewalInterval)

                    val renewed = renewRaw(lockKey, ownerId, ttl)
                    if (!renewed) {
                        logger.warn(
                            "Lost lease while holding lock '$lockName' (ownerId=$ownerId). " +
                                    "Another instance may have acquired it."
                        )
                        break
                    }

                    logger.trace("Renewed lock '$lockName'")
                }
            } catch (e: CancellationException) {
                // Normal cancellation, don't log
            } catch (e: Exception) {
                logger.error("Watchdog error for lock '$lockName'", e)
            }
        }
    }

    /**
     * Generate Redis key for a lock.
     */
    private fun lockKey(lockName: String): String {
        return "${config.keyPrefix}:lock:$lockName"
    }

    /**
     * Generate Redis key for a leader election.
     */
    private fun leaderKey(leaderKey: String): String {
        return "${config.keyPrefix}:leader:$leaderKey"
    }
}

/**
 * Configuration for Redis-based distributed lock.
 */
data class RedisLockConfig(
    /** Key prefix for all lock-related Redis keys */
    val keyPrefix: String = "celery",

    /** Default lease TTL for locks */
    val defaultLeaseTtl: Duration = 60.seconds,

    /** Default acquisition timeout */
    val defaultAcquireTimeout: Duration = 30.seconds,

    /** Retry interval when waiting for lock */
    val retryInterval: Duration = 100.milliseconds,

    /** Watchdog renewal interval (fraction of TTL) */
    val renewalFraction: Double = 0.33 // Renew at 1/3 of TTL
)