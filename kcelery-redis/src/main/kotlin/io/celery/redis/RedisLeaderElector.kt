// kcelery-redis/src/main/kotlin/io/celery/redis/RedisLeaderElector.kt
package io.celery.redis

import io.celery.lock.*
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.api.coroutines
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Pure Redis/Lettuce-based leader election implementation.
 * Built on top of RedisDistributedLock.
 */
@OptIn(ExperimentalLettuceCoroutinesApi::class)
class RedisLeaderElector(
    private val redisClient: RedisClient,
    private val distributedLock: RedisDistributedLock? = null,
    private val config: RedisLeaderConfig = RedisLeaderConfig()
) : LeaderElector {

    private val logger = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    // Use provided lock or create new one
    private val lock = distributedLock ?: RedisDistributedLock(redisClient)

    private val instanceId = "${config.instancePrefix}-${UUID.randomUUID()}"

    // Track active leadership handles
    private val leadershipHandles = ConcurrentHashMap<String, LockHandle>()

    // Promotion callbacks
    private val promotionCallbacks = ConcurrentHashMap<String, suspend (LockHandle) -> Unit>()

    // Leadership status flows
    private val leadershipFlows = ConcurrentHashMap<String, MutableSharedFlow<Boolean>>()

    // Background campaign jobs
    private val campaignJobs = ConcurrentHashMap<String, Job>()

    /**
     * Campaign for leadership.
     */
    override suspend fun campaign(group: String, ttl: Duration): LockHandle? {
        val handle = lock.tryAcquireLeadership(group, ttl)

        if (handle != null) {
            logger.info("Instance $instanceId became leader for group: $group")

            // Store handle
            leadershipHandles[group] = handle

            // Store leader info in Redis
            storeLeaderInfo(group, handle)

            // Notify promotion callbacks
            promotionCallbacks[group]?.let { callback ->
                scope.launch {
                    try {
                        callback(handle)
                    } catch (e: Exception) {
                        logger.error("Promotion callback failed for group: $group", e)
                    }
                }
            }

            // Emit leadership status
            emitLeadershipChange(group, true)

            // Start background renewal tracking
            startLeadershipMonitor(group, handle)
        }

        return handle
    }

    /**
     * Check if still leader using stored handle.
     */
    override suspend fun isLeader(handle: LockHandle): Boolean {
        return lock.isLocked(handle.lockName) &&
                lock.getLockOwner(handle.lockName) == handle.ownerId
    }

    /**
     * Get current leader ID.
     */
    override suspend fun getLeaderId(group: String): String? {
        return try {
            lock.getLockOwner(group)
        } catch (e: Exception) {
            logger.error("Failed to get leader ID for group: $group", e)
            null
        }
    }

    /**
     * Step down from leadership.
     */
    override suspend fun stepDown(handle: LockHandle) {
        try {
            lock.release(handle)
            leadershipHandles.remove(handle.lockName)

            // Cancel campaign job
            campaignJobs.remove(handle.lockName)?.cancel()

            // Emit leadership change
            emitLeadershipChange(handle.lockName, false)

            logger.info("Instance $instanceId stepped down from group: ${handle.lockName}")

        } catch (e: Exception) {
            logger.error("Failed to step down from group: ${handle.lockName}", e)
        }
    }

    /**
     * Watch leadership changes.
     */
    override fun watchLeadership(group: String): Flow<Boolean> {
        return leadershipFlows.computeIfAbsent(group) {
            MutableSharedFlow<Boolean>(replay = 1, extraBufferCapacity = 10)
        }
    }

    /**
     * Register promotion callback.
     */
    override suspend fun onPromotion(group: String, callback: suspend (LockHandle) -> Unit) {
        promotionCallbacks[group] = callback

        // If already leader, call immediately
        leadershipHandles[group]?.let { handle ->
            callback(handle)
        }
    }

    /**
     * Start continuous leadership campaign.
     * Keeps trying to become leader if not currently leader.
     */
    fun startContinuousCampaign(group: String, ttl: Duration = 30.seconds) {
        val job = scope.launch {
            while (isActive) {
                try {
                    if (!leadershipHandles.containsKey(group)) {
                        campaign(group, ttl)
                    }

                    // Check leadership health
                    leadershipHandles[group]?.let { handle ->
                        if (!isLeader(handle)) {
                            logger.warn("Lost leadership for group: $group")
                            leadershipHandles.remove(group)
                            emitLeadershipChange(group, false)
                        }
                    }

                    delay(config.campaignInterval)

                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    logger.error("Error in continuous campaign for group: $group", e)
                    delay(config.campaignInterval)
                }
            }
        }

        campaignJobs[group] = job
    }

    /**
     * Close leader elector.
     */
    override fun close() {
        // Step down from all leaderships
        leadershipHandles.values.forEach { handle ->
            runBlocking {
                try {
                    stepDown(handle)
                } catch (e: Exception) {
                    logger.error("Failed to step down during close: ${handle.lockName}", e)
                }
            }
        }

        leadershipHandles.clear()
        promotionCallbacks.clear()
        leadershipFlows.clear()
        campaignJobs.values.forEach { it.cancel() }
        campaignJobs.clear()

        scope.cancel()

        // Only close lock if we created it
        if (distributedLock == null) {
            lock.close()
        }

        logger.info("RedisLeaderElector closed for instance: $instanceId")
    }

    // --- Private helpers ---

    private suspend fun storeLeaderInfo(group: String, handle: LockHandle) {
        try {
            val connection = redisClient.connect()
            try {
                val commands = connection.coroutines()
                val key = "${config.keyPrefix}:leader:$group:info"
                commands.hset(
                    key,
                    mapOf(
                        "leader_id" to handle.ownerId,
                        "instance_id" to instanceId,
                        "fencing_token" to handle.fencingToken.toString(),
                        "acquired_at" to java.time.Instant.now().toString()
                    )
                )
                commands.expire(key, 60) // Short TTL, refreshed periodically
            } finally {
                connection.close()
            }
        } catch (e: Exception) {
            logger.error("Failed to store leader info for group: $group", e)
        }
    }

    private fun startLeadershipMonitor(group: String, handle: LockHandle) {
        scope.launch {
            try {
                while (isActive && leadershipHandles.containsKey(group)) {
                    delay(config.healthCheckInterval)

                    if (!isLeader(handle)) {
                        logger.warn("Lost leadership for group: $group")
                        leadershipHandles.remove(group)
                        emitLeadershipChange(group, false)
                        break
                    }

                    // Refresh leader info
                    storeLeaderInfo(group, handle)
                }
            } catch (e: CancellationException) {
                // Normal
            } catch (e: Exception) {
                logger.error("Leadership monitor error for group: $group", e)
            }
        }
    }

    private fun emitLeadershipChange(group: String, isLeader: Boolean) {
        leadershipFlows[group]?.let { flow ->
            scope.launch {
                try {
                    flow.emit(isLeader)
                } catch (e: Exception) {
                    logger.error("Failed to emit leadership change for group: $group", e)
                }
            }
        }
    }
}

data class RedisLeaderConfig(
    val keyPrefix: String = "celery",
    val instancePrefix: String = "celery-leader",
    val campaignInterval: Duration = 10.seconds,
    val healthCheckInterval: Duration = 5.seconds
)