package io.celery.lock

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Returned on successful lock acquisition. Carries a strictly increasing
 * fencing token so downstream resources can reject writes from a stale
 * lock holder (e.g. one whose TTL expired but whose `block()` is still
 * finishing). This is what actually prevents split-brain corruption —
 * the TTL alone only bounds how long a stale holder *might* still be
 * running, it doesn't stop it from writing.
 */
data class LockHandle(
    val lockName: String,
    val ownerId: String,
    val fencingToken: Long
)

/**
 * Distributed lock interface for coordinating operations across instances.
 *
 * Uses fencing tokens to prevent split-brain scenarios where a stale lock
 * holder could corrupt data after its lease has expired.
 */
interface DistributedLock {
    /**
     * Execute a block of code while holding the lock.
     * Automatically acquires and releases the lock.
     * Includes automatic lease renewal via watchdog.
     *
     * @param lockName Unique lock name
     * @param acquireTimeout Maximum time to wait for lock
     * @param leaseTtl How long to hold the lock before automatic release
     * @param block Code to execute under lock, receives LockHandle with fencing token
     * @return Result of the block, or null if lock not acquired within timeout
     */
    suspend fun <T> withLock(
        lockName: String,
        acquireTimeout: Duration = 30.seconds,
        leaseTtl: Duration = 60.seconds,
        block: suspend (LockHandle) -> T
    ): T?

    /**
     * Try to acquire leadership for a group.
     * Leadership is a special long-lived lock with automatic renewal.
     *
     * @param leaderKey Leadership group key
     * @param ttl How long the leadership lasts without renewal
     * @return LockHandle if leadership acquired, null otherwise
     */
    suspend fun tryAcquireLeadership(leaderKey: String, ttl: Duration = 30.seconds): LockHandle?

    /**
     * Release a lock or leadership.
     *
     * @param handle The LockHandle to release
     */
    suspend fun release(handle: LockHandle)

    /**
     * Check if lock is currently held by anyone.
     *
     * @param lockName Unique lock name
     * @return true if lock is held
     */
    suspend fun isLocked(lockName: String): Boolean

    /**
     * Get the current lock holder's owner ID.
     *
     * @param lockName Unique lock name
     * @return Owner ID if lock is held, null otherwise
     */
    suspend fun getLockOwner(lockName: String): String?

    /**
     * Get the current fencing token for a lock.
     *
     * @param lockName Unique lock name
     * @return Current fencing token
     */
    suspend fun getFencingToken(lockName: String): Long

    /**
     * Close lock resources.
     */
    fun close()
}

/**
 * Lock exception.
 */
open class LockException(
    message: String,
    cause: Throwable? = null
) : RuntimeException(message, cause)

class LockAcquisitionException(
    message: String,
    cause: Throwable? = null
) : LockException(message, cause)

class LockReleaseException(
    message: String,
    cause: Throwable? = null
) : LockException(message, cause)

class StaleLockException(
    message: String,
    val expectedToken: Long,
    val actualToken: Long
) : LockException(message)