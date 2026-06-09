package io.celery

data class BrokerRecord(
    val messageId: String,
    val payload: TaskMessage,
    val streamKey: String
)