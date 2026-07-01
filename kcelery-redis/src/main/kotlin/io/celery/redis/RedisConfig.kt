package io.celery.redis

import io.lettuce.core.ClientOptions
import io.lettuce.core.ReadFrom
import io.lettuce.core.SocketOptions
import io.lettuce.core.TimeoutOptions
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * Redis configuration for kCelery.
 * Supports standalone, sentinel, and cluster modes.
 */
data class RedisConfig(
    /** Redis mode */
    val mode: RedisMode = RedisMode.STANDALONE,

    /** Connection URLs */
    val urls: List<String> = listOf("redis://localhost:6379"),

    /** Password for authentication */
    val password: String? = null,

    /** Database index (standalone only) */
    val database: Int = 0,

    /** Client name for identification */
    val clientName: String = "kcelery",

    /** Connection timeout */
    val connectionTimeout: Duration = Duration.ofSeconds(10),

    /** Command timeout */
    val commandTimeout: Duration = Duration.ofSeconds(30),

    /** Key prefix for all Redis keys */
    val keyPrefix: String = "celery",

    /** SSL configuration */
    val ssl: RedisSslConfig? = null,

    /** Connection pool settings */
    val pool: RedisPoolConfig = RedisPoolConfig(),

    /** Read from replica settings */
    val readFrom: ReadFrom = ReadFrom.MASTER,

    /** Auto-reconnect enabled */
    val autoReconnect: Boolean = true,

    /** Sentinel configuration (sentinel mode only) */
    val sentinel: RedisSentinelConfig? = null,

    /** Cluster configuration (cluster mode only) */
    val cluster: RedisClusterConfig? = null
) {
    fun toRedisUri(): String {
        val builder = StringBuilder("redis")
        if (ssl != null) builder.append("s")
        builder.append("://")

        if (password != null) {
            builder.append(":$password@")
        }

        builder.append(urls.first().removePrefix("redis://").removePrefix("rediss://"))

        if (database > 0) {
            builder.append("/$database")
        }

        return builder.toString()
    }

    fun toLettuceClientOptions(): ClientOptions {
        return ClientOptions.builder()
            .socketOptions(
                SocketOptions.builder()
                    .connectTimeout(connectionTimeout)
                    .build()
            )
            .timeoutOptions(
                TimeoutOptions.builder()
                    .timeoutCommands(true)
                    .fixedTimeout(commandTimeout)
                    .build()
            )
            .autoReconnect(autoReconnect)
            .build()
    }
}

enum class RedisMode {
    STANDALONE,
    SENTINEL,
    CLUSTER
}

data class RedisSslConfig(
    val keystorePath: String? = null,
    val keystorePassword: String? = null,
    val truststorePath: String? = null,
    val truststorePassword: String? = null,
    val verifyPeer: Boolean = true
)

data class RedisPoolConfig(
    val maxTotal: Int = 8,
    val maxIdle: Int = 8,
    val minIdle: Int = 2,
    val maxWait: Duration = Duration.ofSeconds(10),
    val testOnBorrow: Boolean = true,
    val testOnReturn: Boolean = false,
    val testWhileIdle: Boolean = true,
    val timeBetweenEvictionRuns: Duration = Duration.ofSeconds(30)
)

data class RedisSentinelConfig(
    val masterId: String = "mymaster",
    val sentinelUrls: List<String>,
    val sentinelPassword: String? = null
)

data class RedisClusterConfig(
    val maxRedirects: Int = 5,
    val validateClusterNodeMembership: Boolean = true
)