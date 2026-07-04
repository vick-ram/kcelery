package io.celery.redis

//@OptIn(ExperimentalLettuceCoroutinesApi::class)
//class RedisConnectionFactory(
//    private val config: RedisConfig
//) : Closeable {
//
//    private val logger = LoggerFactory.getLogger(javaClass)
//
//    private val clientResources: ClientResources = DefaultClientResources.builder()
//        .ioThreadPoolSize(Runtime.getRuntime().availableProcessors())
//        .computationThreadPoolSize(Runtime.getRuntime().availableProcessors())
//        .build()
//
//    private val handle: ConnectionHandle by lazy { connect() }
//
//    fun getCommands(): RedisCoroutinesCommands<String, String> = handle.commands
//
//    suspend fun healthCheck(): Boolean = try {
//        handle.commands.ping() == "PONG"
//    } catch (e: Exception) {
//        logger.error("Redis health check failed", e)
//        false
//    }
//
//    override fun close() {
//        try {
//            handle.close()
//            clientResources.shutdown()
//            logger.info("Redis connection closed")
//        } catch (e: Exception) {
//            logger.error("Error closing Redis connection", e)
//        }
//    }
//
//    private sealed class ConnectionHandle : Closeable {
//        abstract val commands: RedisCoroutinesCommands<String, String>
//
//        class Standalone(
//            val client: RedisClient,
//            val connection: StatefulRedisConnection<String, String>
//        ) : ConnectionHandle() {
//            override val commands = connection.coroutines()
//            override fun close() {
//                connection.close()
//                client.shutdown()
//            }
//        }
//
//        class Sentinel(
//            val client: RedisClient,
//            val connection: StatefulRedisMasterReplicaConnection<String, String>
//        ) : ConnectionHandle() {
//            override val commands = connection.coroutines()
//            override fun close() {
//                connection.close()
//                client.shutdown()
//            }
//        }
//
//        class Cluster(
//            val client: RedisClusterClient,
//            val connection: StatefulRedisClusterConnection<String, String>
//        ) : ConnectionHandle() {
//            override val commands = connection.coroutines()
//            override fun close() {
//                connection.close()
//                client.shutdown()
//            }
//        }
//    }
//
//    private fun connect(): ConnectionHandle = when (config) {
//        is RedisConfig.Standalone -> connectStandalone(config)
//        is RedisConfig.Sentinel   -> connectSentinel(config)
//        is RedisConfig.Cluster    -> connectCluster(config)
//    }
//
//    private fun connectStandalone(cfg: RedisConfig.Standalone): ConnectionHandle.Standalone {
//        val uri = RedisURI.builder()
//            .apply {
//                val base = RedisURI.create(cfg.url)
//                withHost(base.host)
//                withPort(base.port)
//                cfg.password?.let { withPassword(it.toCharArray()) }
//                withDatabase(cfg.database)
//                withClientName(cfg.clientName)
//                withSsl(cfg.ssl != null)
//            }
//            .build()
//
//        val client = RedisClient.create(clientResources, uri).also {
//            it.options = cfg.toClientOptions()
//        }
//        val connection = client.connect()
//        logger.info("Standalone Redis connected to ${uri.host}:${uri.port} db=${cfg.database}")
//        return ConnectionHandle.Standalone(client, connection)
//    }
//
//    private fun connectSentinel(cfg: RedisConfig.Sentinel): ConnectionHandle.Sentinel {
//        val firstUrl = RedisURI.create(cfg.sentinelUrls.first())
//        val sentinelUri = RedisURI.Builder
//            .sentinel(firstUrl.host, firstUrl.port, cfg.masterId)
//            .apply {
//                cfg.sentinelUrls.drop(1).forEach { raw ->
//                    val u = RedisURI.create(raw)
//                    withSentinel(u.host, u.port)
//                }
//                cfg.password?.let { withPassword(it.toCharArray()) }
//                cfg.sentinelPassword?.let { withPassword(it.toCharArray()) }
//                withDatabase(cfg.database)
//                withClientName(cfg.clientName)
//                withSsl(cfg.ssl != null)
//            }
//            .build()
//
//        val client = RedisClient.create(clientResources).also {
//            it.options = cfg.toClientOptions()
//        }
//        val connection = MasterReplica.connect(client, StringCodec.UTF8, sentinelUri).also {
//            it.readFrom = cfg.readFrom
//        }
//        logger.info("Sentinel Redis connected, master='${cfg.masterId}', sentinels=${cfg.sentinelUrls.size}")
//        return ConnectionHandle.Sentinel(client, connection)
//    }
//
//    private fun connectCluster(cfg: RedisConfig.Cluster): ConnectionHandle.Cluster {
//        val uris = cfg.urls.map { raw ->
//            val base = RedisURI.create(raw)
//            RedisURI.builder()
//                .withHost(base.host)
//                .withPort(base.port)
//                .apply {
//                    cfg.password?.let { withPassword(it.toCharArray()) }
//                    withClientName(cfg.clientName)
//                    withSsl(cfg.ssl != null)
//                }
//                .build()
//        }
//
//        val client = RedisClusterClient.create(clientResources, uris).also {
//            it.setOptions(cfg.toClusterClientOptions())
//        }
//        val connection = client.connect().also {
//            it.readFrom = cfg.readFrom
//        }
//        logger.info("Cluster Redis connected, nodes=${cfg.urls.size}, maxRedirects=${cfg.maxRedirects}")
//        return ConnectionHandle.Cluster(client, connection)
//    }
//}

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.BaseRedisCoroutinesCommands
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.cluster.api.coroutines
import io.lettuce.core.cluster.api.coroutines.RedisClusterCoroutinesCommands
import io.lettuce.core.codec.StringCodec
import io.lettuce.core.masterreplica.MasterReplica
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection
import io.lettuce.core.resource.ClientResources
import io.lettuce.core.resource.DefaultClientResources
import org.slf4j.LoggerFactory
import java.io.Closeable

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class RedisConnectionFactory(
    private val config: RedisConfig
) : Closeable {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val clientResources: ClientResources = DefaultClientResources.builder()
        .ioThreadPoolSize(Runtime.getRuntime().availableProcessors())
        .computationThreadPoolSize(Runtime.getRuntime().availableProcessors())
        .build()

    private val handle: ConnectionHandle by lazy { connect() }

    /**
     * Exposes the common denominator command interface (good for ping, auth, etc.)
     */
    fun getCommonCommands(): BaseRedisCoroutinesCommands<String, String> = handle.commands

    /**
     * Type-safe access for Standalone / Sentinel installations
     */
    fun getCommands(): RedisCoroutinesCommands<String, String> {
        return when (val h = handle) {
            is ConnectionHandle.Standalone -> h.commands
            is ConnectionHandle.Sentinel -> h.commands
            is ConnectionHandle.Cluster -> throw IllegalStateException("Cannot get standard commands in Cluster mode. Use getClusterCommands().")
        }
    }

    /**
     * Type-safe access for Cluster installations
     */
    fun getClusterCommands(): RedisClusterCoroutinesCommands<String, String> {
        return when (val h = handle) {
            is ConnectionHandle.Cluster -> h.commands
            else -> throw IllegalStateException("Cannot get cluster commands in non-cluster mode.")
        }
    }

    suspend fun healthCheck(): Boolean = try {
        handle.commands.ping() == "PONG"
    } catch (e: Exception) {
        logger.error("Redis health check failed", e)
        false
    }

    override fun close() {
        try {
            handle.close()
            clientResources.shutdown()
            logger.info("Redis connection factory shut down successfully")
        } catch (e: Exception) {
            logger.error("Error closing Redis connection", e)
        }
    }

    private sealed class ConnectionHandle : Closeable {
        abstract val commands: BaseRedisCoroutinesCommands<String, String>

        class Standalone(
            val client: RedisClient,
            val connection: StatefulRedisConnection<String, String>
        ) : ConnectionHandle() {
            override val commands: RedisCoroutinesCommands<String, String> = connection.coroutines()
            override fun close() {
                connection.close()
                client.shutdown()
            }
        }

        class Sentinel(
            val client: RedisClient,
            val connection: StatefulRedisMasterReplicaConnection<String, String>
        ) : ConnectionHandle() {
            override val commands: RedisCoroutinesCommands<String, String> = connection.coroutines()
            override fun close() {
                connection.close()
                client.shutdown()
            }
        }

        class Cluster(
            val client: RedisClusterClient,
            val connection: StatefulRedisClusterConnection<String, String>
        ) : ConnectionHandle() {
            // FIX: This now compiles perfectly because RedisClusterCoroutinesCommands extends BaseRedisCoroutinesCommands
            override val commands: RedisClusterCoroutinesCommands<String, String> = connection.coroutines()
            override fun close() {
                connection.close()
                client.shutdown()
            }
        }
    }

    private fun connect(): ConnectionHandle = when (config) {
        is RedisConfig.Standalone -> connectStandalone(config)
        is RedisConfig.Sentinel   -> connectSentinel(config)
        is RedisConfig.Cluster    -> connectCluster(config)
    }

    private fun connectStandalone(cfg: RedisConfig.Standalone): ConnectionHandle.Standalone {
        val uri = RedisURI.builder()
            .apply {
                val base = RedisURI.create(cfg.url)
                withHost(base.host)
                withPort(base.port)
                cfg.password?.let { withPassword(it.toCharArray()) }
                withDatabase(cfg.database)
                withClientName(cfg.clientName)
                withSsl(cfg.ssl != null)
            }
            .build()

        val client = RedisClient.create(clientResources, uri).also {
            it.options = cfg.toClientOptions()
        }
        val connection = client.connect()
        logger.info("Standalone Redis connected to ${uri.host}:${uri.port} db=${cfg.database}")
        return ConnectionHandle.Standalone(client, connection)
    }

    private fun connectSentinel(cfg: RedisConfig.Sentinel): ConnectionHandle.Sentinel {
        val firstUrl = RedisURI.create(cfg.sentinelUrls.first())
        val sentinelUri = RedisURI.Builder
            .sentinel(firstUrl.host, firstUrl.port, cfg.masterId)
            .apply {
                cfg.sentinelUrls.drop(1).forEach { raw ->
                    val u = RedisURI.create(raw)
                    withSentinel(u.host, u.port)
                }
                cfg.password?.let { withPassword(it.toCharArray()) }
                cfg.sentinelPassword?.let { withPassword(it.toCharArray()) }
                withDatabase(cfg.database)
                withClientName(cfg.clientName)
                withSsl(cfg.ssl != null)
            }
            .build()

        val client = RedisClient.create(clientResources).also {
            it.options = cfg.toClientOptions()
        }
        val connection = MasterReplica.connect(client, StringCodec.UTF8, sentinelUri).also {
            it.readFrom = cfg.readFrom
        }
        logger.info("Sentinel Redis connected, master='${cfg.masterId}', sentinels=${cfg.sentinelUrls.size}")
        return ConnectionHandle.Sentinel(client, connection)
    }

    private fun connectCluster(cfg: RedisConfig.Cluster): ConnectionHandle.Cluster {
        val uris = cfg.urls.map { raw ->
            val base = RedisURI.create(raw)
            RedisURI.builder()
                .withHost(base.host)
                .withPort(base.port)
                .apply {
                    cfg.password?.let { withPassword(it.toCharArray()) }
                    withClientName(cfg.clientName)
                    withSsl(cfg.ssl != null)
                }
                .build()
        }

        val client = RedisClusterClient.create(clientResources, uris).also {
            it.setOptions(cfg.toClusterClientOptions())
        }
        val connection = client.connect().also {
            it.readFrom = cfg.readFrom
        }
        logger.info("Cluster Redis connected, nodes=${cfg.urls.size}, maxRedirects=${cfg.maxRedirects}")
        return ConnectionHandle.Cluster(client, connection)
    }
}
