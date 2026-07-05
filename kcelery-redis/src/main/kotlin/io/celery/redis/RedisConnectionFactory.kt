package io.celery.redis

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
import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.PooledObject
import org.apache.commons.pool2.impl.DefaultPooledObject
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.time.Duration

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class RedisConnectionFactory(
    private val config: RedisConfig,
    poolConfig: RedisPoolConfig = RedisPoolConfig()
) : Closeable {

    private val logger = LoggerFactory.getLogger(javaClass)

    private val clientResources: ClientResources = DefaultClientResources.builder()
        .ioThreadPoolSize(Runtime.getRuntime().availableProcessors())
        .computationThreadPoolSize(Runtime.getRuntime().availableProcessors())
        .build()

    private val poolHandle: PoolHandle by lazy { setupPool(poolConfig) }

    data class RedisPoolConfig(
        val maxTotal: Int = Runtime.getRuntime().availableProcessors() * 2,
        val maxIdle: Int = Runtime.getRuntime().availableProcessors(),
        val minIdle: Int = 2,
        val maxWait: Duration = Duration.ofSeconds(5),
        val testOnBorrow: Boolean = true,
        val testOnReturn: Boolean = false,
        val testWhileIdle: Boolean = true,
        val timeBetweenEvictionRuns: Duration = Duration.ofSeconds(30),
        val numTestsPerEvictionRun: Int = 3
    )

    /**
     * Borrow a connection from the pool, run[action], and return it.
     * Works uniformly for all modes - cluster commands extend
     * [RedisCoroutinesCommands] so on separate withClusterCommands() needded.
     * [borrowObject] is a blocking Commons Pool so we dispatch to IO.
     */
    suspend fun <T> withConnection(
        action: suspend (BaseRedisCoroutinesCommands<String, String>) -> T
    ): T {
        val pooled = acquireConnection()
        try {
            return action(pooled.commands)
        } finally {
            releaseConnection(pooled)
        }
    }

    suspend fun <T> withCommands(
        action: suspend (RedisCoroutinesCommands<String, String>) -> T
    ): T {
        val pooled = acquireConnection()
        try {
            val commands = when (pooled) {
                is PooledConnection.Standalone -> pooled.commands
                is PooledConnection.Sentinel -> pooled.commands
                is PooledConnection.Cluster ->
                    throw IllegalStateException("Use withClusterCommands() for cluster mode")
            }
            return action(commands)
        } finally {
            releaseConnection(pooled)
        }
    }

    suspend fun <T> withClusterCommands(
        action: suspend (RedisClusterCoroutinesCommands<String, String>) -> T
    ): T {
        val pooled = acquireConnection()
        try {
            val commands = when (pooled) {
                is PooledConnection.Cluster -> pooled.commands
                else -> throw IllegalStateException("Use withCommands() for non-cluster mode")
            }
            return action(commands)
        } finally {
            releaseConnection(pooled)
        }
    }

    suspend fun healthCheck(): Boolean = try {
        withConnection { commands ->
            commands.ping() == "PONG"
        }
    } catch (e: Exception) {
        logger.error("Redis health check failed", e)
        false
    }

    fun getPoolStats(): PoolStats = poolHandle.stats()

    override fun close() {
        try {
            poolHandle.close()
            clientResources.shutdown()
            logger.info("Redis connection pool shut down")
        } catch (e: Exception) {
            logger.error("Error closing Redis connection pool", e)
        }
    }

    private fun acquireConnection(): PooledConnection {
        return when (val handle = poolHandle) {
            is PoolHandle.Standalone -> {
                val conn = handle.pool.borrowObject()
                PooledConnection.Standalone(conn, conn.coroutines())
            }

            is PoolHandle.Sentinel -> {
                val conn = handle.pool.borrowObject()
                PooledConnection.Sentinel(conn, conn.coroutines())
            }

            is PoolHandle.Cluster -> {
                val conn = handle.pool.borrowObject()
                PooledConnection.Cluster(conn, conn.coroutines())
            }
        }
    }

    private fun releaseConnection(pooled: PooledConnection) {
        when (val handle = poolHandle) {
            is PoolHandle.Standalone -> handle.pool.returnObject((pooled as PooledConnection.Standalone).connection)
            is PoolHandle.Sentinel -> handle.pool.returnObject((pooled as PooledConnection.Sentinel).connection)
            is PoolHandle.Cluster -> handle.pool.returnObject((pooled as PooledConnection.Cluster).connection)
        }
    }

    private fun <T> applyPoolConfig(poolConfig: RedisPoolConfig): GenericObjectPoolConfig<T> {
        return GenericObjectPoolConfig<T>().apply {
            maxTotal = poolConfig.maxTotal
            maxIdle = poolConfig.maxIdle
            minIdle = poolConfig.minIdle
            setMaxWait(poolConfig.maxWait)
            testOnBorrow = poolConfig.testOnBorrow
            testOnReturn = poolConfig.testOnReturn
            testWhileIdle = poolConfig.testWhileIdle
            timeBetweenEvictionRuns = poolConfig.timeBetweenEvictionRuns
            numTestsPerEvictionRun = poolConfig.numTestsPerEvictionRun
        }
    }

    private fun setupPool(poolConfig: RedisPoolConfig): PoolHandle {
        return when (config) {
            is RedisConfig.Standalone -> {
                val uri = buildStandaloneUri(config)
                val client = RedisClient.create(clientResources, uri).also { it.options = config.toClientOptions() }
                val pool =
                    GenericObjectPool(object : BasePooledObjectFactory<StatefulRedisConnection<String, String>>() {
                        override fun create() = client.connect(StringCodec.UTF8)
                        override fun wrap(obj: StatefulRedisConnection<String, String>): PooledObject<StatefulRedisConnection<String, String>> =
                            DefaultPooledObject(obj)

                        override fun validateObject(p: PooledObject<StatefulRedisConnection<String, String>>): Boolean =
                            p.`object`.isOpen

                        override fun destroyObject(p: PooledObject<StatefulRedisConnection<String, String>>) =
                            p.`object`.close()
                    }, applyPoolConfig(poolConfig))
                PoolHandle.Standalone(client, pool)
            }

            is RedisConfig.Sentinel -> {
                val sentinelUri = buildSentinelUri(config)
                val client = RedisClient.create(clientResources).also { it.options = config.toClientOptions() }
                val pool = GenericObjectPool(object :
                    BasePooledObjectFactory<StatefulRedisMasterReplicaConnection<String, String>>() {
                    override fun create() = MasterReplica.connect(client, StringCodec.UTF8, sentinelUri)
                        .also { it.readFrom = config.readFrom }

                    override fun wrap(obj: StatefulRedisMasterReplicaConnection<String, String>): PooledObject<StatefulRedisMasterReplicaConnection<String, String>> =
                        DefaultPooledObject(obj)

                    override fun validateObject(p: PooledObject<StatefulRedisMasterReplicaConnection<String, String>>): Boolean =
                        p.`object`.isOpen

                    override fun destroyObject(p: PooledObject<StatefulRedisMasterReplicaConnection<String, String>>) =
                        p.`object`.close()
                }, applyPoolConfig(poolConfig))
                PoolHandle.Sentinel(client, pool)
            }

            is RedisConfig.Cluster -> {
                val uris = config.urls.map { raw ->
                    val base = RedisURI.create(raw)
                    RedisURI.builder().withHost(base.host).withPort(base.port).apply {
                        config.password?.let { withPassword(it.toCharArray()) }
                        withClientName(config.clientName)
                        withSsl(config.ssl != null)
                    }.build()
                }
                val client = RedisClusterClient.create(clientResources, uris)
                    .also { it.setOptions(config.toClusterClientOptions()) }
                val pool = GenericObjectPool(object :
                    BasePooledObjectFactory<StatefulRedisClusterConnection<String, String>>() {
                    override fun create() = client.connect(StringCodec.UTF8).also { it.readFrom = config.readFrom }
                    override fun wrap(obj: StatefulRedisClusterConnection<String, String>): PooledObject<StatefulRedisClusterConnection<String, String>> =
                        DefaultPooledObject(obj)

                    override fun validateObject(p: PooledObject<StatefulRedisClusterConnection<String, String>>): Boolean =
                        p.`object`.isOpen

                    override fun destroyObject(p: PooledObject<StatefulRedisClusterConnection<String, String>>) =
                        p.`object`.close()
                }, applyPoolConfig(poolConfig))
                PoolHandle.Cluster(client, pool)
            }
        }
    }

    private fun buildStandaloneUri(cfg: RedisConfig.Standalone): RedisURI {
        val base = RedisURI.create(cfg.url)
        return RedisURI.builder().withHost(base.host).withPort(base.port).apply {
            cfg.password?.let { withPassword(it.toCharArray()) }
            withDatabase(cfg.database)
            withClientName(cfg.clientName)
            withSsl(cfg.ssl != null)
        }.build()
    }

    private fun buildSentinelUri(cfg: RedisConfig.Sentinel): RedisURI {
        val firstUrl = RedisURI.create(cfg.sentinelUrls.first())
        return RedisURI.Builder.sentinel(firstUrl.host, firstUrl.port, cfg.masterId).apply {
            cfg.sentinelUrls.drop(1).forEach { raw ->
                val u = RedisURI.create(raw)
                withSentinel(u.host, u.port)
            }
            cfg.password?.let { withPassword(it.toCharArray()) }
            cfg.sentinelPassword?.let { withPassword(it.toCharArray()) }
            withDatabase(cfg.database)
            withClientName(cfg.clientName)
            withSsl(cfg.ssl != null)
        }.build()
    }

    private sealed class PoolHandle {
        abstract fun stats(): PoolStats
        abstract fun close()

        class Standalone(
            val client: RedisClient,
            val pool: GenericObjectPool<StatefulRedisConnection<String, String>>
        ) : PoolHandle() {
            override fun stats(): PoolStats = PoolStats(pool.numActive, pool.numIdle, pool.maxTotal, pool.maxIdle)

            override fun close() { pool.close(); client.shutdown() }
        }

        class Sentinel(
            val client: RedisClient,
            val pool: GenericObjectPool<StatefulRedisMasterReplicaConnection<String, String>>
        ) : PoolHandle() {
            override fun stats() = PoolStats(pool.numActive, pool.numIdle, pool.maxTotal, pool.maxIdle)
            override fun close() { pool.close(); client.shutdown() }
        }

        class Cluster(
            val client: RedisClusterClient,
            val pool: GenericObjectPool<StatefulRedisClusterConnection<String, String>>
        ) : PoolHandle() {
            override fun stats() = PoolStats(pool.numActive, pool.numIdle, pool.maxTotal, pool.maxIdle)
            override fun close() { pool.close(); client.shutdown() }
        }
    }

    private sealed class PooledConnection {
        abstract val commands: BaseRedisCoroutinesCommands<String, String>

        class Standalone(
            val connection: StatefulRedisConnection<String, String>,
            override val commands: RedisCoroutinesCommands<String, String>
        ) : PooledConnection()

        class Sentinel(
            val connection: StatefulRedisMasterReplicaConnection<String, String>,
            override val commands: RedisCoroutinesCommands<String, String>
        ) : PooledConnection()

        class Cluster(
            val connection: StatefulRedisClusterConnection<String, String>,
            override val commands: RedisClusterCoroutinesCommands<String, String>
        ) : PooledConnection()
    }

    data class PoolStats(
        val active: Int,
        val idle: Int,
        val maxTotal: Int,
        val maxIdle: Int
    )
}