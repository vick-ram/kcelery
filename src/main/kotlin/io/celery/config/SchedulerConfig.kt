package io.celery.config

import io.lettuce.core.ExperimentalLettuceCoroutinesApi

import io.celery.core.CronScheduler
import io.celery.core.Clock
import io.celery.core.RedisDistributedLockManager
import io.celery.core.SchedulerMetrics
import io.lettuce.core.RedisClient
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisCoroutinesCommands
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.serialization.json.Json
import org.redisson.Redisson
import org.redisson.api.RedissonClient
import org.redisson.config.Config

@OptIn(ExperimentalLettuceCoroutinesApi::class)
data class SchedulerConfig(
    val redisUrl: String = "redis://localhost:6379",
    val workerThreads: Int = Runtime.getRuntime().availableProcessors(),
    val schedulerName: String = "default",
    val clock: Clock = Clock.utc(),
    val json: Json = Json {
        ignoreUnknownKeys = true
        prettyPrint = false
    }
)

class SchedulerFactory {

    fun create(config: SchedulerConfig, meterRegistry: MeterRegistry? = null): CronScheduler {

        // Redis connection
        val redisClient = RedisClient.create(config.redisUrl)
        val redisCommands: RedisCoroutinesCommands<String, String> = redisClient.connect().coroutines()


        // Redisson for distributed locking
        val redissonConfig = Config().apply {
            useSingleServer().address = config.redisUrl
        }
        val redissonClient: RedissonClient = Redisson.create(redissonConfig)

        // Distributed lock manager
        val lockManager = RedisDistributedLockManager(redissonClient)

        // Metrics
        val metrics = SchedulerMetrics(
            meterRegistry ?: PrometheusMeterRegistry(PrometheusConfig.DEFAULT),
            config.schedulerName
        )

        return CronScheduler(
            clock = config.clock,
            workerThreads = config.workerThreads,
            json = config.json,
            redis = redisCommands,
            lockManager = lockManager,
            metrics = metrics,
            schedulerName = config.schedulerName
        )
    }
}