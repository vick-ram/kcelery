package io.celery.broker

import io.celery.model.BrokerRecord
import io.celery.model.TaskMessage
import io.celery.redis.RedisBroker
import io.celery.test.RedisTestBase
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.serialization.json.JsonPrimitive
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds

@OptIn(ExperimentalLettuceCoroutinesApi::class)
class MessageBrokerTest : RedisTestBase() {

    private lateinit var broker: RedisBroker

    @BeforeEach
    override fun setUp() {
        super.setUp()
        broker = RedisBroker(
           redisCommands,
            json
        )
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun `should publish and consume message`() = runTest {
        val task = TaskMessage(
            taskName = "test-task",
            args = listOf(JsonPrimitive("arg1")),
            kwargs = mapOf("key" to JsonPrimitive("value"))
        )

        broker.publish(task, "default")

        val records = broker.consume("default", "test-group", "consumer-1")
            .take(1)
            .toList()

        assertEquals(1, records.size)
        assertEquals(task.taskName, records[0].payload.taskName)
        assertEquals("default", records[0].payload.queue)
    }

//    @Test
//    fun `should schedule delayed task`() = runTest {
//        val task = TaskMessage(
//            taskName = "delayed-task",
//            eta = System.currentTimeMillis() + 5000 // 5 seconds from now
//        )
//
//        broker.scheduleTask(task)
//
//        delay(1000.milliseconds) // Wait 1 second, task should not be ready yet
//
//        val immediateRecords = broker.consume("default", "delayed-group", "consumer-1")
//            .take(1)
//            .toList()
//
//        assertTrue(immediateRecords.isEmpty(), "Task should not be consumed before ETA")
//    }

    @Test
    fun should_schedule_delayed_task() = runTest {
        // 1. Clean your Redis state here if possible to avoid stale data

        val task = TaskMessage(
            taskName = "delayed-task",
            eta = System.currentTimeMillis() + 5000 // 5 seconds from now
        )
        broker.scheduleTask(task)

        // 2. Collect into a list on a background scope
        val immediateRecords = mutableListOf<BrokerRecord>()
        val collectionJob = launch {
            broker.consume("default", "delayed-group", "consumer-1")
                .collect { immediateRecords.add(it) }
        }

        // 3. Advance virtual time by 1 second
        delay(1000.milliseconds)

        // 4. Verify nothing was captured, then kill the infinite consumer
        assertTrue(immediateRecords.isEmpty(), "Task should not be consumed before ETA")
        collectionJob.cancel()
    }

    @Test
    fun `should acknowledge message`() = runTest {
        val task = TaskMessage(taskName = "ack-test")
        broker.publish(task, "default")

        val record = broker.consume("default", "ack-group", "consumer-1")
            .take(1)
            .toList()
            .first()

        broker.acknowledge(record.streamKey, "ack-group", record.messageId)
    }

//    @Test
//    fun `should handle expired task`() = runTest {
//        val task = TaskMessage(
//            taskName = "expired-task",
//            expires = System.currentTimeMillis() - 1000 // Already expired
//        )
//
//        broker.publish(task, "default")
//
//        // Should not receive expired task
//        val records = broker.consume("default", "expired-group", "consumer-1")
//            .take(1)
//            .toList()
//
//        assertTrue(records.isEmpty(), "Expired task should not be consumed")
//    }

    @Test
    fun `should handle expired task`() = runTest {
        // 1. Arrange an expired task
        val task = TaskMessage(
            taskName = "expired-task",
            expires = System.currentTimeMillis() - 1000 // Already expired
        )

        broker.publish(task, "default")

        // 2. Launch the infinite consumer flow in a background coroutine
        val records = mutableListOf<BrokerRecord>()
        val consumerJob = launch {
            broker.consume("default", "expired-group", "consumer-1")
                .collect { records.add(it) }
        }

        // 3. Advance virtual time to give the consumer loops time to run
        // (This flushes the main consumer loop and background recovery workers)
        advanceTimeBy(2000)

        // 4. Assert that the expired task was filtered out and never emitted
        assertTrue(records.isEmpty(), "Expired task should not be consumed")

        // 5. Clean up the infinite background worker
        consumerJob.cancel()
    }
}