package io.celery.worker

import io.celery.model.BrokerRecord
import io.celery.model.CeleryTask
import io.celery.redis.MessageBroker
import io.celery.redis.ResultBackend
import io.celery.core.SerializerRegistry
import io.celery.model.TaskMessage
import io.celery.model.TaskRegistry
import io.celery.model.TaskResult
import io.celery.model.TaskState
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import kotlin.time.Duration.Companion.milliseconds

class Worker(
    private val name: String,
    private val queues: List<String>,
    private val concurrency: Int,
    private val broker: MessageBroker,
    private val backend: ResultBackend?,
    private val taskRegistry: TaskRegistry,
    private val serializerRegistry: SerializerRegistry // Add SerializerRegistry as a constructor parameter
) {
    private val logger = LoggerFactory.getLogger(Worker::class.java)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())
    private val activeJobs = ConcurrentHashMap<String, Job>()
    // private val serializerRegistry = SerializerRegistry() // Remove instantiation here

    fun start() {
        logger.info("Starting worker: $name with $concurrency consumers")

        // Distribute concurrency across queues
        val consumersPerQueue = maxOf(1, concurrency / queues.size)

        queues.forEach { queue ->
            repeat(consumersPerQueue) { consumerIndex ->
                scope.launch {
                    val consumerName = "$name-${queue}-${consumerIndex + 1}"

                    broker.consume(queue, "celery-workers", consumerName)
                        .collect { record ->
                            processTask(record, consumerName)
                        }
                }
            }
        }

        // Start heartbeat
        scope.launch {
            while (currentCoroutineContext().isActive) {
                logger.debug("Worker $name heartbeat - active jobs: ${activeJobs.size}")
                delay(10000.milliseconds)
            }
        }
    }

    private suspend fun processTask(record: BrokerRecord, consumerName: String) {
        val task = record.payload
        val celeryTask = taskRegistry.getTask(task.taskName)

        if (celeryTask == null) {
            logger.error("Unknown task: ${task.taskName}")
            broker.reject(record.streamKey, "celery-workers", record.messageId, requeue = false)
            return
        }

        val job = scope.launch {
            try {
                // Store started state
                backend?.storeResult(
                    task.id,
                    TaskResult(
                        taskId = task.id,
                        state = TaskState.STARTED,
                        workerName = name
                    )
                )

                // Execute with timeout
                val result = withTimeout(300000.milliseconds) { // 5 minute timeout
                    celeryTask.run(task.args, task.kwargs)
                }

                // Store success
                val serializedResult = serializerRegistry.serialize(result)
                backend?.storeResult(
                    task.id,
                    TaskResult(
                        taskId = task.id,
                        state = TaskState.SUCCESS,
                        result = serializedResult,
                        completedAt = Instant.now(),
                        workerName = name
                    )
                )

                broker.acknowledge(record.streamKey, "celery-workers", record.messageId)

            } catch (e: Exception) {
                handleTaskFailure(task, celeryTask, e, record)
            }
        }

        activeJobs[task.id] = job
    }

    private suspend fun handleTaskFailure(
        task: TaskMessage,
        celeryTask: CeleryTask<*>,
        exception: Exception,
        record: BrokerRecord
    ) {
        logger.error("Task ${task.taskName}[${task.id}] failed", exception)

        if (task.retries < task.maxRetries) {
            val retryDelay = celeryTask.onRetry(exception, task.retries + 1)
            val retryTask = task.copy(
                retries = task.retries + 1,
                eta = System.currentTimeMillis() + retryDelay * 1000
            )

            broker.publish(retryTask, task.queue)
            broker.acknowledge(record.streamKey, "celery-workers", record.messageId)

            backend?.storeResult(
                task.id,
                TaskResult(
                    taskId = task.id,
                    state = TaskState.RETRY,
                    traceback = exception.stackTraceToString(),
                    workerName = name
                )
            )
        } else {
            // Max retries exceeded
            backend?.storeResult(
                task.id,
                TaskResult(
                    taskId = task.id,
                    state = TaskState.FAILURE,
                    traceback = exception.stackTraceToString(),
                    completedAt = Instant.now(),
                    workerName = name
                )
            )

            // Move to dead letter queue
            broker.reject(record.streamKey, "celery-workers", record.messageId, requeue = false)
        }
    }

    fun revokeTask(taskId: String): Boolean {
        val job = activeJobs[taskId]
        if (job != null) {
            job.cancel()
            activeJobs.remove(taskId)
            return true
        }
        return false
    }

    fun stop() {
        scope.cancel()
        activeJobs.clear()
    }
}