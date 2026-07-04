package io.celery.redis

import io.lettuce.core.ClientOptions
import io.lettuce.core.ReadFrom
import io.lettuce.core.SocketOptions
import io.lettuce.core.SslOptions
import io.lettuce.core.TimeoutOptions
import io.lettuce.core.cluster.ClusterClientOptions
import java.time.Duration

/**
 * Sealed hierarchy so illegal states are unrepresentable:
 * sentinel config can't accidentally appear on a standalone connection, etc.
 * Common options (timeouts, SSL, client name) live on the base class.
 */
sealed class RedisConfig {
    abstract val clientName: String
    abstract val connectionTimeout: Duration
    abstract val commandTimeout: Duration
    abstract val keyPrefix: String
    abstract val autoReconnect: Boolean
    abstract val ssl: RedisSslConfig?

    data class Standalone(
        val url: String = "redis://localhost:6379",
        val password: String? = null,
        val database: Int = 0,
        override val clientName: String = "kcelery",
        override val connectionTimeout: Duration = Duration.ofSeconds(10),
        override val commandTimeout: Duration = Duration.ofSeconds(30),
        override val keyPrefix: String = "celery",
        override val autoReconnect: Boolean = true,
        override val ssl: RedisSslConfig? = null
    ) : RedisConfig()

    data class Sentinel(
        val masterId: String = "mymaster",
        val sentinelUrls: List<String>,
        val password: String? = null,
        val sentinelPassword: String? = null,
        val database: Int = 0,
        // ReadFrom.UPSTREAM replaces deprecated ReadFrom.MASTER
        val readFrom: ReadFrom = ReadFrom.UPSTREAM,
        override val clientName: String = "kcelery",
        override val connectionTimeout: Duration = Duration.ofSeconds(10),
        override val commandTimeout: Duration = Duration.ofSeconds(30),
        override val keyPrefix: String = "celery",
        override val autoReconnect: Boolean = true,
        override val ssl: RedisSslConfig? = null
    ) : RedisConfig() {
        init {
            require(sentinelUrls.isNotEmpty()) { "At least one sentinel URL required" }
        }
    }

    data class Cluster(
        val urls: List<String>,
        val password: String? = null,
        val maxRedirects: Int = 5,
        val validateClusterNodeMembership: Boolean = true,
        val readFrom: ReadFrom = ReadFrom.UPSTREAM,
        override val clientName: String = "kcelery",
        override val connectionTimeout: Duration = Duration.ofSeconds(10),
        override val commandTimeout: Duration = Duration.ofSeconds(30),
        override val keyPrefix: String = "celery",
        override val autoReconnect: Boolean = true,
        override val ssl: RedisSslConfig? = null
    ) : RedisConfig() {
        init {
            require(urls.isNotEmpty()) { "At least one cluster node URL required" }
        }
    }

    /** Shared Lettuce ClientOptions for standalone and sentinel connections. */
    fun toClientOptions(): ClientOptions = ClientOptions.builder()
        .socketOptions(SocketOptions.builder().connectTimeout(connectionTimeout).build())
        .timeoutOptions(
            TimeoutOptions.builder()
                .timeoutCommands(true)
                .fixedTimeout(commandTimeout)
                .build()
        )
        .autoReconnect(autoReconnect)
        .apply { ssl?.let { sslOptions(it.toLettuceOptions()) } }
        .build()

    /** ClusterClientOptions for cluster mode — includes maxRedirects and membership validation. */
    fun toClusterClientOptions(): ClusterClientOptions {
        val clusterConfig = this as? Cluster
        return ClusterClientOptions.builder()
            .socketOptions(SocketOptions.builder().connectTimeout(connectionTimeout).build())
            .timeoutOptions(
                TimeoutOptions.builder()
                    .timeoutCommands(true)
                    .fixedTimeout(commandTimeout)
                    .build()
            )
            .autoReconnect(autoReconnect)
            .maxRedirects(clusterConfig?.maxRedirects ?: 5)
            .validateClusterNodeMembership(clusterConfig?.validateClusterNodeMembership ?: true)
            .apply { ssl?.let { sslOptions(it.toLettuceOptions()) } }
            .build()
    }
}

data class RedisSslConfig(
    val keystorePath: String? = null,
    val keystorePassword: String? = null,
    val truststorePath: String? = null,
    val truststorePassword: String? = null,
    val verifyPeer: Boolean = true
) {
    fun toLettuceOptions(): SslOptions = SslOptions.builder()
        .apply {
            if (!verifyPeer) {
                // Disable hostname verification — use only in dev/test
                jdkSslProvider()
            }
            keystorePath?.let {
                keystore(java.io.File(it), keystorePassword?.toCharArray() ?: charArrayOf())
            }
            truststorePath?.let {
                truststore(java.io.File(it), truststorePassword ?: "")
            }
        }
        .build()
}