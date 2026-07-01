package io.celery.redis

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.api.coroutines
import io.lettuce.core.cluster.api.coroutines.RedisClusterCoroutinesCommands
import io.lettuce.core.masterreplica.MasterReplica
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection
import io.lettuce.core.resource.ClientResources
import io.lettuce.core.resource.DefaultClientResources
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.util.concurrent.ConcurrentHashMap

/**
 * Factory for creating and managing Redis connections.
 * Supports standalone, sentinel, and cluster modes.
 */
@OptIn(ExperimentalLettuceCoroutinesApi::class)
class RedisConnectionFactory(
    private val config: RedisConfig
) : Closeable {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val clientResources: ClientResources = DefaultClientResources.builder()
        .ioThreadPoolSize(Runtime.getRuntime().availableProcessors())
        .computationThreadPoolSize(Runtime.getRuntime().availableProcessors())
        .build()

    private val connections = ConcurrentHashMap<String, Any>()

    /**
     * Get coroutine commands for Redis operations.
     */
    suspend fun getCommands(): RedisCoroutinesCommands<String, String> {
        return getStandaloneConnection().coroutines()
    }

    /**
     * Get cluster commands (if in cluster mode).
     */
    suspend fun getClusterCommands(): RedisClusterCoroutinesCommands<String, String>? {
        if (config.mode != RedisMode.CLUSTER) return null
        return getClusterConnection().coroutines()
    }

    /**
     * Get master-replica commands (if in sentinel mode).
     */
    suspend fun getMasterReplicaCommands(): RedisCoroutinesCommands<String, String>? {
        if (config.mode != RedisMode.SENTINEL) return null
        return getSentinelConnection().coroutines()
    }

    /**
     * Get appropriate commands based on mode.
     */
    suspend fun getAppropriateCommands(): Any {
        return when (config.mode) {
            RedisMode.STANDALONE -> getCommands()
            RedisMode.CLUSTER -> getClusterCommands()!!
            RedisMode.SENTINEL -> getMasterReplicaCommands()!!
        }
    }

    private fun getStandaloneConnection(): StatefulRedisConnection<String, String> {
        return connections.computeIfAbsent("standalone") {
            val redisUri = RedisURI.create(config.toRedisUri())
            val client = RedisClient.create(clientResources, redisUri)
            client.options = config.toLettuceClientOptions()

            val connection = client.connect()
            logger.info("Created standalone Redis connection to ${redisUri.host}:${redisUri.port}")
            connection
        } as StatefulRedisConnection<String, String>
    }

    private fun getClusterConnection(): StatefulRedisClusterConnection<String, String> {
        return connections.computeIfAbsent("cluster") {
            val uris = config.urls.map { RedisURI.create(it) }
            val client = RedisClusterClient.create(clientResources, uris)
            client.options = config.toLettuceClientOptions()

            val connection = client.connect()
            logger.info("Created Redis cluster connection with ${uris.size} nodes")
            connection
        } as StatefulRedisClusterConnection<String, String>
    }

    private fun getSentinelConnection(): StatefulRedisMasterReplicaConnection<String, String> {
        return connections.computeIfAbsent("sentinel") {
            val sentinelConfig = config.sentinel
                ?: throw IllegalStateException("Sentinel config required for sentinel mode")

            val uris = sentinelConfig.sentinelUrls.map { RedisURI.create(it) }
            val client = RedisClient.create(clientResources)

            val connection = MasterReplica.connect(
                client,
                client.codec,
                uris
            ).apply {
                setReadFrom(config.readFrom)
            }

            logger.info("Created Redis sentinel connection with master: ${sentinelConfig.masterId}")
            connection
        } as StatefulRedisMasterReplicaConnection<String, String>
    }

    /**
     * Check connection health.
     */
    suspend fun healthCheck(): Boolean {
        return try {
            when (config.mode) {
                RedisMode.STANDALONE -> {
                    getCommands().ping() == "PONG"
                }
                RedisMode.CLUSTER -> {
                    getClusterCommands()?.ping() == "PONG"
                }
                RedisMode.SENTINEL -> {
                    getMasterReplicaCommands()?.ping() == "PONG"
                }
            }
        } catch (e: Exception) {
            logger.error("Redis health check failed", e)
            false
        }
    }

    /**
     * Close all connections.
     */
    override fun close() {
        connections.values.forEach { connection ->
            try {
                when (connection) {
                    is StatefulRedisConnection<*, *> -> connection.close()
                    is StatefulRedisClusterConnection<*, *> -> connection.close()
                    is StatefulRedisMasterReplicaConnection<*, *> -> connection.close()
                }
            } catch (e: Exception) {
                logger.error("Failed to close connection", e)
            }
        }
        connections.clear()
        clientResources.shutdown()
        logger.info("Redis connections closed")
    }
}