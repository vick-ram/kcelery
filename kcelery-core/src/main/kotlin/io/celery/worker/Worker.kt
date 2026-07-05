package io.celery.worker

import io.celery.backend.ResultBackend
import io.celery.backend.ResultStatus
import io.celery.backend.TaskResult
import io.celery.broker.BrokerRecord
import io.celery.broker.MessageBroker
import io.celery.task.CeleryTask
import io.celery.task.TaskConfig
import io.celery.task.TaskContext
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.sync.Semaphore
import kotlinx.serialization.json.Json
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/**
 * Worker that consumes tasks from broker and executes them.
 */
class Worker(
    /** Unique worker name */
    val name: String,

    /** Queues to consume from */
    private val queues: List<String> = listOf("default"),

    /** Number of concurrent task executions */
    private val concurrency: Int = Runtime.getRuntime().availableProcessors(),

    /** Message broker */
    private val broker: MessageBroker,

    /** Result backend (optional) */
    private val backend: ResultBackend? = null,

    /** Task registry mapping task names to implementations */
    private val taskRegistry: Map<String, CeleryTask<*>> = emptyMap(),

    /** JSON serializer */
    private val json: Json = Json { ignoreUnknownKeys = true },

    /** Consumer group name */
    private val consumerGroup: String = "celery-workers"
) {
    private val logger = LoggerFactory.getLogger(javaClass)
    private val scope = CoroutineScope(Dispatchers.Default + SupervisorJob())

    private val isRunning = AtomicBoolean(false)
    private val activeTasks = AtomicInteger(0)
    private val maxConcurrency = Semaphore(concurrency)
    private val activeJobs = ConcurrentHashMap<String, Job>()

    // Metrics
    private val tasksProcessed = AtomicInteger(0)
    private val tasksFailed = AtomicInteger(0)
    private val tasksRetried = AtomicInteger(0)

    /** Check if the worker is currently running and accepting tasks */
    val isActive: Boolean
        get() = isRunning.get() && scope.isActive

    /**
     * Start the worker.
     */
    fun start() {
        if (!isRunning.compareAndSet(false, true)) {
            logger.warn("Worker $name already running")
            return
        }

        logger.info("Starting worker: $name with concurrency: $concurrency")

        // Distribute concurrency across queues
        val consumersPerQueue = maxOf(1, concurrency / queues.size)

        queues.forEach { queue ->
            repeat(consumersPerQueue) { consumerIndex ->
                scope.launch {
                    val consumerName = "$name-$queue-${consumerIndex + 1}"
                    consumeFromQueue(queue, consumerName)
                }
            }
        }

        // Start heartbeat
        scope.launch {
            while (isActive) {
                logger.debug("Worker $name heartbeat - active: ${activeTasks.get()}, " +
                        "processed: ${tasksProcessed.get()}, failed: ${tasksFailed.get()}")
                delay(30.seconds)
            }
        }
    }

    /**
     * Stop the worker gracefully.
     */
    suspend fun stop(timeout: Duration = 30.seconds) {
        logger.info("Stopping worker: $name")
        isRunning.set(false)

        // Wait for active tasks
        withTimeout(timeout) {
            while (activeTasks.get() > 0) {
                delay(100.milliseconds)
            }
        }

        // Cancel all running jobs
        activeJobs.values.forEach { it.cancel() }
        activeJobs.clear()

        scope.cancel()
        logger.info("Worker $name stopped. Stats: processed=${tasksProcessed.get()}, " +
                "failed=${tasksFailed.get()}, retried=${tasksRetried.get()}")
    }

    /**
     * Revoke a running task.
     */
    fun revokeTask(taskId: String): Boolean {
        val job = activeJobs.remove(taskId)
        if (job != null) {
            job.cancel()
            logger.info("Revoked task: $taskId")
            return true
        }
        return false
    }

    private fun CoroutineScope.consumeFromQueue(queue: String, consumerName: String) {
        launch {
            try {
                broker.consume(
                    queue = queue,
                    consumerGroup = consumerGroup,
                    consumerName = consumerName
                ).flowOn(Dispatchers.IO).collect { record ->
                    if (isRunning.get()) {
                        processRecord(record, consumerName)
                    }
                }
            } catch (_: CancellationException) {
                // Normal shutdown
            } catch (e: Exception) {
                logger.error("Consumer $consumerName failed", e)
                if (isRunning.get()) {
                    delay(5.seconds)
                    consumeFromQueue(queue, consumerName)
                }
            }
        }
    }

    private fun CoroutineScope.processRecord(record: BrokerRecord, consumerName: String) {
        launch {
            maxConcurrency.acquire()
            try {
                activeTasks.incrementAndGet()

                val job = launch {
                    executeTask(record, consumerName)
                }

                activeJobs[record.payload.id] = job
                job.join()

            } catch (e: Exception) {
                logger.error("Failed to process record: ${record.payload.id}", e)
            } finally {
                activeTasks.decrementAndGet()
                activeJobs.remove(record.payload.id)
                maxConcurrency.release()
            }
        }
    }

    private suspend fun <T : Any> executeSafely(
        task: CeleryTask<T>,
        context: TaskContext,
        record: BrokerRecord
    ) {
        val taskId = context.taskId
        val taskName = task.name

        backend?.storeResult(
            taskId,
            TaskResult(taskId = taskId, status = ResultStatus.STARTED, worker = name)
        )

        try {
            val result = withTimeout(task.executionTimeout) {
                task.beforeRun(context)
                val r = task.run(context)
                task.onSuccess(context, r)  // inside timeout, before ack — if it throws, we handle it
                task.afterRun(context)
                r
            }

            tasksProcessed.incrementAndGet()
            if (task.autoAck) broker.ack(record)

            backend?.storeResult(
                taskId,
                TaskResult(
                    taskId      = taskId,
                    status      = ResultStatus.SUCCESS,
                    result      = json.encodeToJsonElement(task.serializer, result),
                    completedAt = Instant.now(),
                    worker      = name
                )
            )
            logger.debug("Task completed: $taskName [$taskId]")

        } catch (e: TimeoutCancellationException) {
            // Must come before CancellationException — it's a subclass and would
            // be swallowed by the broader catch if the order were reversed.
            logger.warn("Task timed out: $taskName [$taskId]")
            handleTaskFailure(task, context, record, e)

        } catch (e: CancellationException) {
            // Structured cancellation (worker shutdown, scope cancel) — don't retry
            broker.nack(record, requeue = false)
            backend?.storeResult(
                taskId,
                TaskResult(taskId = taskId, status = ResultStatus.REVOKED, worker = name)
            )
            throw e  // must rethrow so structured concurrency propagates correctly

        } catch (e: Exception) {
            logger.error("Task failed: $taskName [$taskId]", e)
            handleTaskFailure(task, context, record, e)
        }
    }

    private suspend fun executeTask(record: BrokerRecord, consumerName: String) {
        val taskMessage = record.payload
        val taskId      = taskMessage.id
        val taskName    = taskMessage.taskName

        logger.debug("Executing task: $taskName [$taskId]")

        if (backend?.isRevoked(taskId) == true) {
            logger.info("Task $taskId is revoked, skipping")
            broker.ack(record)
            backend.storeResult(
                taskId,
                TaskResult(taskId = taskId, status = ResultStatus.REVOKED, worker = name)
            )
            return
        }

        val task = taskRegistry[taskName]
        if (task == null) {
            logger.error("Unknown task: $taskName")
            broker.nack(record, requeue = false)
            backend?.storeResult(
                taskId,
                TaskResult(
                    taskId    = taskId,
                    status    = ResultStatus.REJECTED,
                    error     = "Unknown task: $taskName",
                    errorType = "UnknownTaskException",
                    worker    = name
                )
            )
            return
        }

        executeSafely(task, taskMessage.toTaskContext(consumerName), record)
    }

    private suspend fun handleTaskFailure(
        task: CeleryTask<*>,
        context: TaskContext,
        record: BrokerRecord,
        exception: Exception
    ) {
        val taskId = record.payload.id

        if (task.isRetryable(exception) && RetryPolicy.shouldRetry(
                TaskConfig(maxRetries = task.maxRetries),
                context.attempt, exception)
        ) {

            // Retry
            tasksRetried.incrementAndGet()
            val retryDelay = task.onRetry(exception, context.attempt + 1)

            backend?.storeResult(
                taskId,
                TaskResult(
                    taskId = taskId,
                    status = ResultStatus.RETRY,
                    error = exception.message,
                    errorType = exception.javaClass.name,
                    worker = name
                )
            )

            broker.nack(record, requeue = true)
            logger.info("Task $taskId will retry in ${retryDelay}s")

        } else {
            // Final failure
            tasksFailed.incrementAndGet()

            broker.nack(record, requeue = false)

            backend?.storeResult(
                taskId,
                TaskResult(
                    taskId = taskId,
                    status = ResultStatus.FAILURE,
                    error = exception.message,
                    errorType = exception.javaClass.name,
                    traceback = exception.stackTraceToString(),
                    completedAt = Instant.now(),
                    worker = name
                )
            )

            task.onFailure(context, exception)
        }
    }

    /**
     * Worker statistics.
     */
    fun getStats(): WorkerStats = WorkerStats(
        name = name,
        activeTasks = activeTasks.get(),
        processedTasks = tasksProcessed.get(),
        failedTasks = tasksFailed.get(),
        retriedTasks = tasksRetried.get(),
        queues = queues,
        concurrency = concurrency
    )
}

data class WorkerStats(
    val name: String,
    val activeTasks: Int,
    val processedTasks: Int,
    val failedTasks: Int,
    val retriedTasks: Int,
    val queues: List<String>,
    val concurrency: Int
)