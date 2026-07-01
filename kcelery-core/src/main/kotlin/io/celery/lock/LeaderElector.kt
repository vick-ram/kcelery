package io.celery.lock

import kotlinx.coroutines.flow.Flow
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Leader election interface for coordinating leadership across instances.
 */
interface LeaderElector {
    /**
     * Campaign for leadership of a group.
     * 
     * @param group The leadership group name
     * @param ttl How long the leadership lasts without renewal
     * @return true if leadership was acquired
     */
    suspend fun campaign(group: String, ttl: Duration = 30.seconds): LockHandle?

    /**
     * Check if this instance is the current leader.
     * 
     * @param handle The LockHandle from campaign()
     * @return true if this instance is still leader
     */
    suspend fun isLeader(handle: LockHandle): Boolean

    /**
     * Get the current leader ID.
     * 
     * @param group The leadership group name
     * @return Current leader owner ID, or null if no leader
     */
    suspend fun getLeaderId(group: String): String?

    /**
     * Step down from leadership.
     * 
     * @param handle The LockHandle to release
     */
    suspend fun stepDown(handle: LockHandle)

    /**
     * Watch for leadership changes.
     * Emits true when this instance becomes leader, false when it loses leadership.
     * 
     * @param group The leadership group name
     * @return Flow of leadership status
     */
    fun watchLeadership(group: String): Flow<Boolean>

    /**
     * Register callback for leadership promotion.
     * 
     * @param group The leadership group name
     * @param callback Called when this instance becomes leader
     */
    suspend fun onPromotion(group: String, callback: suspend (LockHandle) -> Unit)

    /**
     * Close leader election resources.
     */
    fun close()
}

/**
 * Leader election exception.
 */
class LeaderElectionException(
    message: String,
    cause: Throwable? = null
) : RuntimeException(message, cause)