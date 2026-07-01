package io.celery.broker

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Duration.Companion.minutes

/**
 * Configuration for message broker behavior.
 */
data class BrokerConfig(
    /** Broker type identifier */
    val type: String = "redis",

    /** Connection URL(s) */
    val connectionUrls: List<String> = listOf("redis://localhost:6379"),

    /** Maximum number of messages to prefetch */
    val prefetchCount: Int = 10,

    /** Poll timeout for message consumption */
    val pollTimeout: Duration = 5.seconds,

    /** Message TTL in broker */
    val messageTtl: Duration = 24.minutes * 60, // 24 hours

    /** Result TTL in backend */
    val resultTtl: Duration = 1.minutes * 60, // 1 hour

    /** Whether to use consumer groups (for load balancing) */
    val useConsumerGroups: Boolean = true,

    /** Consumer group name */
    val consumerGroup: String = "celery-workers",

    /** Key prefix for broker entities */
    val keyPrefix: String = "celery",

    /** Whether to auto-create streams/queues */
    val autoCreateResources: Boolean = true,

    /** Maximum retries for broker operations */
    val maxBrokerRetries: Int = 3,

    /** Broker operation timeout */
    val operationTimeout: Duration = 10.seconds,

    /** Connection pool size */
    val connectionPoolSize: Int = 8,

    /** Whether to enable SSL/TLS */
    val useSsl: Boolean = false,

    /** Authentication credentials */
    val credentials: BrokerCredentials? = null,

    /** Custom broker properties */
    val properties: Map<String, String> = emptyMap()
)

data class BrokerCredentials(
    val username: String,
    val password: String
)

/**
 * Enumeration of supported broker types.
 */
enum class BrokerType(val identifier: String) {
    REDIS("redis"),
    RABBITMQ("rabbitmq"),
    KAFKA("kafka"),
    IN_MEMORY("in-memory")
}