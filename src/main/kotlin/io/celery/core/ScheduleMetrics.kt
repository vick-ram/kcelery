package io.celery.core

import io.micrometer.core.instrument.MeterRegistry
import java.util.concurrent.atomic.AtomicLong

class SchedulerMetrics(
    private val registry: MeterRegistry,
    private val schedulerName: String = "default"
) {
    private val taskExecutions = AtomicLong(0)
    private val taskFailures = AtomicLong(0)
    private val taskSkips = AtomicLong(0)
    private val taskRetries = AtomicLong(0)
    private val queueSize = AtomicLong(0)

    init {
        registry.gauge("celery.scheduler.queue.size", queueSize)
        registry.gauge("celery.scheduler.executions.total", taskExecutions)
        registry.gauge("celery.scheduler.failures.total", taskFailures)
        registry.gauge("celery.scheduler.skips.total", taskSkips)
        registry.gauge("celery.scheduler.retries.total", taskRetries)
    }

    fun recordExecution(taskName: String, duration: java.time.Duration) {
        taskExecutions.incrementAndGet()
        registry.timer("celery.task.execution",
            "scheduler", schedulerName,
            "task", taskName
        ).record(duration)
    }

    fun recordFailure(taskName: String, error: Throwable) {
        taskFailures.incrementAndGet()
        registry.counter("celery.task.failure",
            "scheduler", schedulerName,
            "task", taskName,
            "error", error.javaClass.simpleName
        ).increment()
    }

    fun recordSkip(taskName: String, reason: String) {
        taskSkips.incrementAndGet()
        registry.counter("celery.task.skip",
            "scheduler", schedulerName,
            "task", taskName,
            "reason", reason
        ).increment()
    }

    fun recordRetry(taskName: String, attempt: Int) {
        taskRetries.incrementAndGet()
        registry.counter("celery.task.retry",
            "scheduler", schedulerName,
            "task", taskName,
            "attempt", attempt.toString()
        ).increment()
    }

    fun updateQueueSize(size: Int) {
        queueSize.set(size.toLong())
    }
}