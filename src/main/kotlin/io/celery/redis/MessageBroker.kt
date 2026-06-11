package io.celery.redis

import io.celery.model.BrokerRecord
import io.celery.model.TaskMessage
import kotlinx.coroutines.flow.Flow
import java.time.Duration

interface MessageBroker {
    suspend fun publish(task: TaskMessage, queue: String)
    fun consume(queue: String, consumerGroup: String, consumerName: String): Flow<BrokerRecord>
    suspend fun acknowledge(streamKey: String, group: String, messageId: String)
    suspend fun reject(streamKey: String, group: String, messageId: String, requeue: Boolean)
    suspend fun claimPending(queue: String, group: String, consumerName: String, minIdleTime: Duration)
    suspend fun scheduleTask(task: TaskMessage)
    suspend fun close()
}