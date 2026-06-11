package io.celery.model

data class BrokerRecord(
    val messageId: String,
    val payload: TaskMessage,
    val streamKey: String
)