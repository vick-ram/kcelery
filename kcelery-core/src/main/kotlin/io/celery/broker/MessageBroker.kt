package io.celery.broker

import io.celery.task.TaskMessage
import kotlinx.coroutines.flow.Flow
import kotlin.time.Duration

/**
 * Message broker record containing the consumed message.
 */
data class BrokerRecord(
    /** Broker-specific message ID */
    val messageId: String,

    /** Deserialized task message */
    val payload: TaskMessage,

    /** Stream/queue key */
    val streamKey: String,

    /** Consumer group name */
    val group: String,

    /** Consumer name */
    val consumer: String,

    /** When the message was received */
    val receivedAt: kotlinx.datetime.Instant = kotlinx.datetime.Clock.System.now()
)

/**
 * Abstract message broker interface.
 * Implementations provide concrete transport (Redis Streams, RabbitMQ, Kafka, etc.)
 */
interface MessageBroker {
    /**
     * Enqueue a task message to a queue.
     * 
     * @param task The task message to enqueue
     * @param queue Target queue name
     * @throws BrokerException if enqueue fails
     */
    suspend fun enqueue(task: TaskMessage, queue: String)

    /**
     * Consume messages from a queue as a Flow.
     * 
     * @param queue Queue name to consume from
     * @param consumerGroup Consumer group for load balancing
     * @param consumerName Unique consumer name
     * @param batchSize Maximum messages per poll
     * @param pollTimeout Maximum time to wait for messages
     * @return Flow of broker records
     */
    fun consume(
        queue: String,
        consumerGroup: String,
        consumerName: String,
        batchSize: Int = 10,
        pollTimeout: Duration = Duration.seconds(5)
    ): Flow<BrokerRecord>

    /**
     * Acknowledge successful processing of a message.
     * 
     * @param record The broker record to acknowledge
     */
    suspend fun ack(record: BrokerRecord)

    /**
     * Negative-acknowledge (reject) a message.
     * 
     * @param record The broker record to reject
     * @param requeue Whether to requeue for retry
     */
    suspend fun nack(record: BrokerRecord, requeue: Boolean = true)

    /**
     * Move a message to the dead letter queue.
     * 
     * @param record The broker record
     * @param reason Reason for dead-lettering
     */
    suspend fun deadLetter(record: BrokerRecord, reason: String)

    /**
     * Schedule a task for future execution.
     * 
     * @param task The task message to schedule
     */
    suspend fun scheduleTask(task: TaskMessage)

    /**
     * Get queue depth/backlog.
     * 
     * @param queue Queue name
     * @return Number of pending messages
     */
    suspend fun queueSize(queue: String): Long

    /**
     * Purge all messages from a queue.
     * 
     * @param queue Queue name
     */
    suspend fun purgeQueue(queue: String)

    /**
     * Close broker connections.
     */
    suspend fun close()

    /**
     * Check broker health.
     * 
     * @return true if broker is healthy
     */
    suspend fun healthCheck(): Boolean
}

/**
 * Base exception for broker-related errors.
 */
open class BrokerException(
    message: String,
    cause: Throwable? = null
) : RuntimeException(message, cause)

class BrokerConnectionException(message: String, cause: Throwable? = null) :
    BrokerException(message, cause)

class BrokerSerializationException(message: String, cause: Throwable? = null) :
    BrokerException(message, cause)

class BrokerPublishException(message: String, cause: Throwable? = null) :
    BrokerException(message, cause)

class BrokerConsumeException(message: String, cause: Throwable? = null) :
    BrokerException(message, cause)