# KCelery - Kotlin Distributed Task Queue & Scheduler

[![Kotlin](https://img.shields.io/badge/Kotlin-1.9%252B-blue.svg)](https://kotlinlang.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](https://github.com/yourorg/kcelery/actions)


A Kotlin-native distributed task queue for the JVM, inspired by Python's Celery. Built from the ground up with coroutines-first design — no thread-blocking wrappers, no borrowed async APIs, just structured concurrency throughout.

---

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Modules](#modules)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
    - [Redis — Standalone](#redis--standalone)
    - [Redis — Sentinel](#redis--sentinel)
    - [Redis — Cluster](#redis--cluster)
    - [Connection Pool](#connection-pool)
- [Defining Tasks](#defining-tasks)
    - [Basic Task](#basic-task)
    - [Lifecycle Hooks](#lifecycle-hooks)
    - [Retry Behaviour](#retry-behaviour)
- [Dispatching Tasks](#dispatching-tasks)
    - [Immediate Dispatch](#immediate-dispatch)
    - [Delayed Dispatch (ETA)](#delayed-dispatch-eta)
    - [Expiring Tasks](#expiring-tasks)
    - [Task Chains and Groups](#task-chains-and-groups)
- [Workers](#workers)
    - [Starting a Worker](#starting-a-worker)
    - [Consumer Groups](#consumer-groups)
    - [Concurrency Model](#concurrency-model)
- [Scheduling](#scheduling)
    - [Fixed-Delay Scheduler](#fixed-delay-scheduler)
    - [Cron Scheduler](#cron-scheduler)
    - [Cron Expression Reference](#cron-expression-reference)
    - [Misfire Policies](#misfire-policies)
- [Result Backend](#result-backend)
    - [Storing and Fetching Results](#storing-and-fetching-results)
    - [Result Statuses](#result-statuses)
- [Distributed Locking](#distributed-locking)
    - [withLock](#withlock)
    - [Fencing Tokens](#fencing-tokens)
    - [Leader Election](#leader-election)
- [Dead Letter Queue](#dead-letter-queue)
    - [Inspecting DLQ Records](#inspecting-dlq-records)
    - [Replaying Failed Tasks](#replaying-failed-tasks)
    - [Purging Old Records](#purging-old-records)
- [Metrics](#metrics)
- [Testing](#testing)
    - [In-Memory Broker](#in-memory-broker)
    - [Virtual Time and Schedulers](#virtual-time-and-schedulers)
- [Broker-Agnostic Design](#broker-agnostic-design)
- [RabbitMQ Support](#rabbitmq-support)
- [Error Handling Reference](#error-handling-reference)
- [Contributing](#contributing)

---

## Features

- **Coroutines-first** — every API is `suspend`; no `runBlocking`, no thread-blocking wrappers
- **Redis Streams** — consumer-group-based message delivery with at-least-once semantics
- **Distributed locking** with watchdog TTL renewal and fencing tokens for stale-writer rejection
- **Leader election** with automatic lease renewal and voluntary step-down
- **Cron and fixed-delay schedulers** with configurable misfire policies
- **Dead letter queue** with replay, pagination, and time-based purge
- **Pluggable broker and result backend** — swap Redis for RabbitMQ without touching task code
- **Connection pooling** via Apache Commons Pool 2 for all Redis modes (standalone, sentinel, cluster)
- **Gradle multi-module** — depend only on the modules you need

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                      kcelery-core                       │
│                                                         │
│  MessageBroker   ResultBackend   DistributedLock        │
│  LeaderElector   TaskMessage     CeleryTask<T>          │
│  CronScheduler   FixedDelayScheduler                    │
└────────────┬───────────────────────────┬────────────────┘
             │                           │
    ┌────────▼────────┐         ┌────────▼────────┐
    │  kcelery-redis  │         │ kcelery-rabbitmq │
    │                 │         │   (planned)      │
    │ RedisMessageBroker        └─────────────────┘
    │ RedisResultBackend
    │ RedisDistributedLock
    │ RedisLeaderElector
    │ RedisDeadLetterQueue
    │ RedisConnectionFactory
    └────────┬────────┘
             │
    ┌────────▼────────┐         ┌─────────────────┐
    │ kcelery-metrics │         │ kcelery-testkit  │
    │                 │         │                  │
    │ Micrometer      │         │ InMemoryBroker   │
    │ instrumentation │         │ InMemoryBackend  │
    └─────────────────┘         └─────────────────┘
```

**Core interfaces** live in `kcelery-core` and know nothing about Redis or any other broker. All runtime implementations are in separate modules. This means:

- `kcelery-redis` implements `MessageBroker` with Redis Streams + consumer groups
- A future `kcelery-rabbitmq` module implements the same interface via AMQP
- `kcelery-redis` also provides `DistributedLock`/`LeaderElector` — these can be mixed with any broker since locking and queuing are independent concerns
- Tests use `kcelery-testkit`'s in-memory fakes — no Redis required

---

## Modules

| Module | Purpose | Key classes |
|---|---|---|
| `kcelery-core` | Broker-agnostic contracts, task model, schedulers | `CeleryTask`, `MessageBroker`, `DistributedLock`, `CronScheduler`, `FixedDelayScheduler` |
| `kcelery-redis` | Redis implementation of all core interfaces | `RedisMessageBroker`, `RedisDistributedLock`, `RedisLeaderElector`, `RedisConnectionFactory` |
| `kcelery-metrics` | Micrometer instrumentation | `MicrometerTaskMetrics` |
| `kcelery-testkit` | In-memory fakes for unit tests | `InMemoryMessageBroker`, `InMemoryResultBackend`, `InMemoryLock` |

---

## Installation

```kotlin
// settings.gradle.kts
dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}
```

```kotlin
// build.gradle.kts
dependencies {
    // Core contracts — always required
    implementation("io.harrier:kcelery-core:0.1.0")

    // Redis backend — add if using Redis
    implementation("io.harrier:kcelery-redis:0.1.0")

    // Metrics — optional
    implementation("io.harrier:kcelery-metrics:0.1.0")

    // Test utilities — test scope only
    testImplementation("io.harrier:kcelery-testkit:0.1.0")
}
```

---

## Quick Start

```kotlin
// 1. Define a task
class SendEmailTask : CeleryTask<Unit>(
    name = "send_email",
    serializer = Unit.serializer()
) {
    override suspend fun run(context: TaskContext) {
        val to = context.kwargs["to"]!!.jsonPrimitive.content
        emailService.send(to, subject = "Hello")
    }
}

// 2. Build the app
val config = RedisConfig.Standalone(url = "redis://localhost:6379")
val factory = RedisConnectionFactory(config)
val broker = RedisMessageBroker(factory)

val app = CeleryApp(
    broker = broker,
    tasks  = listOf(SendEmailTask())
)

// 3. Dispatch a task
app.send(
    taskName = "send_email",
    kwargs   = mapOf("to" to JsonPrimitive("user@example.com"))
)

// 4. Start a worker (in a separate process or coroutine)
app.worker("worker-1").start()
```

---

## Configuration

### Redis — Standalone

```kotlin
val config = RedisConfig.Standalone(
    url               = "redis://localhost:6379",
    password          = "secret",
    database          = 0,
    clientName        = "kcelery",
    connectionTimeout = Duration.ofSeconds(10),
    commandTimeout    = Duration.ofSeconds(30),
    keyPrefix         = "myapp",
    autoReconnect     = true
)
```

### Redis — Sentinel

```kotlin
val config = RedisConfig.Sentinel(
    masterId       = "mymaster",
    sentinelUrls   = listOf(
        "redis://sentinel-1:26379",
        "redis://sentinel-2:26379",
        "redis://sentinel-3:26379"
    ),
    password         = "redis-password",       // Redis node password
    sentinelPassword = "sentinel-password",    // Sentinel AUTH password (separate)
    database         = 0,
    readFrom         = ReadFrom.UPSTREAM
)
```

> **Note:** `password` and `sentinelPassword` are distinct. Sentinel nodes can have their own `requirepass` separate from the Redis data nodes. Mixing them up silently breaks authentication.

### Redis — Cluster

```kotlin
val config = RedisConfig.Cluster(
    urls     = listOf(
        "redis://node-1:7000",
        "redis://node-2:7001",
        "redis://node-3:7002"
    ),
    password                      = "secret",
    maxRedirects                  = 5,
    validateClusterNodeMembership = true,
    readFrom                      = ReadFrom.UPSTREAM
)
```

### Connection Pool

All modes use Apache Commons Pool 2 under the hood. Tune via `RedisPoolConfig`:

```kotlin
val poolConfig = RedisConnectionFactory.RedisPoolConfig(
    maxTotal               = Runtime.getRuntime().availableProcessors() * 2,
    maxIdle                = Runtime.getRuntime().availableProcessors(),
    minIdle                = 2,
    maxWait                = Duration.ofSeconds(5),
    testOnBorrow           = true,   // ping before use
    testOnReturn           = false,
    testWhileIdle          = true,
    timeBetweenEvictionRuns = Duration.ofSeconds(30)
)

val factory = RedisConnectionFactory(config, poolConfig)
```

`borrowObject()` is dispatched on `Dispatchers.IO` so a blocked borrow (pool exhausted) never starves `Dispatchers.Default`.

### SSL

```kotlin
val ssl = RedisSslConfig(
    keystorePath      = "/etc/ssl/keystore.jks",
    keystorePassword  = "keypass",
    truststorePath    = "/etc/ssl/truststore.jks",
    truststorePassword = "trustpass",
    verifyPeer        = true   // set false only in dev/test
)

val config = RedisConfig.Standalone(url = "rediss://localhost:6380", ssl = ssl)
```

---

## Defining Tasks

### Basic Task

```kotlin
@Serializable
data class OrderPayload(val orderId: String, val amount: Double)

class ProcessOrderTask : CeleryTask<OrderPayload>(
    name             = "process_order",
    maxRetries       = 3,
    executionTimeout = 2.minutes,
    serializer       = OrderPayload.serializer()
) {
    override suspend fun run(context: TaskContext): OrderPayload {
        val orderId = context.kwargs["orderId"]!!.jsonPrimitive.content
        // ... processing logic
        return OrderPayload(orderId, 99.99)
    }
}
```

**`executionTimeout`** is the wall-clock limit for a single execution attempt. It is independent of `maxRetries` — a task that times out will be retried up to `maxRetries` times, each with its own timeout window.

### Lifecycle Hooks

```kotlin
class AuditedTask : CeleryTask<String>(
    name       = "audited_task",
    serializer = String.serializer()
) {
    override suspend fun beforeRun(context: TaskContext) {
        auditLog.record("started", context.taskId)
    }

    override suspend fun run(context: TaskContext): String = "done"

    override suspend fun onSuccess(context: TaskContext, result: String) {
        metrics.increment("task.success", "task" to name)
    }

    override suspend fun onFailure(context: TaskContext, exception: Throwable) {
        alerting.notify("Task $name failed: ${exception.message}")
    }

    override suspend fun afterRun(context: TaskContext) {
        // called after success or failure, guaranteed
        auditLog.record("finished", context.taskId)
    }
}
```

Execution order: `beforeRun` → `run` → `onSuccess` → `afterRun`. On failure: `beforeRun` → `run` (throws) → `onFailure` → `afterRun`.

### Retry Behaviour

```kotlin
class RetryableTask : CeleryTask<Unit>(
    name       = "retryable",
    maxRetries = 5,
    serializer = Unit.serializer()
) {
    override suspend fun run(context: TaskContext) {
        if (context.attempt < 3) throw IOException("not ready yet")
    }

    // Exponential backoff — override for custom strategy
    override fun onRetry(exception: Exception, retries: Int): Long = when {
        retries <= 1 -> 10L
        retries <= 3 -> 10L * retries
        else         -> minOf(10L * retries, 3600L) // cap at 1 hour
    }

    // Only retry transient errors, not programming errors
    override fun isRetryable(exception: Exception): Boolean =
        exception is IOException || exception is TimeoutException
}
```

`context.attempt` is 0-indexed: `0` on first attempt, `1` on first retry, etc.

---

## Dispatching Tasks

### Immediate Dispatch

```kotlin
app.send(
    taskName = "process_order",
    kwargs   = buildJsonObject {
        put("orderId", "ORD-123")
        put("amount", 99.99)
    },
    queue    = "orders",
    priority = 5
)
```

### Delayed Dispatch (ETA)

```kotlin
// Run after a delay
app.send(
    taskName = "send_reminder",
    kwargs   = buildJsonObject { put("userId", "U-42") },
    delay    = 30.minutes
)

// Run at a specific time
app.send(
    taskName = "send_report",
    kwargs   = emptyJsonObject,
    eta      = Instant.now().plus(1, ChronoUnit.DAYS)
)
```

Delayed tasks are parked in a Redis sorted set (score = ETA epoch millis). The broker's scheduler recovery loop polls for due tasks and moves them to the target stream once their ETA is reached.

### Expiring Tasks

```kotlin
app.send(
    taskName = "flash_sale_notification",
    kwargs   = buildJsonObject { put("saleId", "SALE-99") },
    expires  = 2.hours   // discard if not processed within 2 hours
)
```

Workers check `task.isExpired()` on receipt. Expired tasks are acked and discarded rather than executed or dead-lettered.

### Task Chains and Groups

Task chains are expressed via `parentId`/`rootId` on `TaskMessage`. The result backend can be queried to wait for completion:

```kotlin
// Chain: B runs after A completes
val taskA = app.send(taskName = "step_a", kwargs = emptyJsonObject)
app.send(
    taskName  = "step_b",
    kwargs    = emptyJsonObject,
    parentId  = taskA.id,
    rootId    = taskA.id
)

// Poll for result
val result = backend.awaitResult(taskA.id, timeout = 30.seconds)
```

---

## Workers

### Starting a Worker

```kotlin
val worker = app.worker(
    name          = "worker-1",
    queues        = listOf("default", "high-priority"),
    consumerGroup = "kcelery-workers",
    concurrency   = 4,         // parallel task coroutines
    pollTimeout   = 2.seconds
)

// In a coroutine scope
worker.start()

// Graceful shutdown — waits for in-flight tasks to complete
worker.stop()
```

### Consumer Groups

Kcelery uses Redis Streams consumer groups for at-least-once delivery:

- Each worker instance registers as a named consumer in the group
- Messages are delivered to exactly one consumer at a time
- Unacked messages past `pendingMessageTimeout` (default 30s) are reclaimed by the next available consumer
- `broker.ack(record)` removes the message from the pending list; `broker.nack(record, requeue = true)` leaves it pending for redelivery

```kotlin
// Manual ack — set autoAck = false on the task
class CarefulTask : CeleryTask<Unit>(
    name     = "careful",
    autoAck  = false,
    serializer = Unit.serializer()
) {
    override suspend fun run(context: TaskContext) {
        // do work...
        // ack only after external confirmation
        externalSystem.confirm(context.taskId)
    }
}
```

### Concurrency Model

Each worker runs a single `consume()` flow on `Dispatchers.IO`. Individual task executions are launched as child coroutines in a `SupervisorJob` scope, so one failing task doesn't cancel sibling executions. The `concurrency` parameter controls the maximum number of simultaneously running task coroutines per worker.

---

## Scheduling

### Fixed-Delay Scheduler

The delay is measured from the **end** of the previous execution, not its start. This prevents execution overlap when tasks run longer than their interval.

```kotlin
val scheduler = FixedDelayScheduler(
    taskExecutor = { taskName, context -> app.execute(taskName, context) }
)

scheduler.schedule(
    taskName = "cleanup_temp_files",
    delay    = 10.minutes,
    config   = TaskConfig(
        allowConcurrentExecution = false,
        misfirePolicy            = MisfirePolicy.IGNORE
    )
)

scheduler.start()

// Later
scheduler.updateDelay("cleanup_temp_files-fixed-delay-...", 5.minutes)
scheduler.stop()  // waits for in-flight executions to finish
```

### Cron Scheduler

```kotlin
val scheduler = CronScheduler(
    taskExecutor = { taskName, context -> app.execute(taskName, context) }
)

scheduler.schedule(
    taskName       = "generate_daily_report",
    cronExpression = "0 0 8 * * MON-FRI",  // 08:00 on weekdays
    config         = TaskConfig(misfirePolicy = MisfirePolicy.FIRE_ONCE)
)

scheduler.start()
```

The cron scheduler uses a `Channel.CONFLATED` wake-up signal so newly added schedules take effect immediately rather than waiting for the next poll cycle (up to 60s).

### Cron Expression Reference

Kcelery supports both 5-field (standard) and 6-field (with seconds) cron expressions.

```
# 6-field: second minute hour day-of-month month day-of-week
# 5-field: minute hour day-of-month month day-of-week

┌─────────── second (0-59)         [6-field only]
│ ┌───────── minute (0-59)
│ │ ┌─────── hour (0-23)
│ │ │ ┌───── day-of-month (1-31)
│ │ │ │ ┌─── month (1-12)
│ │ │ │ │ ┌─ day-of-week (0-7, SUN-SAT; 0 and 7 both = Sunday)
│ │ │ │ │ │
* * * * * *
```

| Expression | Meaning |
|---|---|
| `* * * * *` | Every minute |
| `0 * * * *` | Every hour on the hour |
| `0 8 * * MON-FRI` | 08:00 on weekdays |
| `0 0 1 * *` | Midnight on the 1st of every month |
| `*/15 * * * *` | Every 15 minutes |
| `0 9,17 * * *` | 09:00 and 17:00 daily |
| `0 0 8 * * MON-FRI` | 08:00 on weekdays (6-field with seconds) |
| `30 0 0 * * *` | 00:00:30 daily (6-field) |

**Day-of-month + day-of-week semantics:** when both fields are non-wildcard, a task runs if **either** condition is met (OR semantics, matching Vixie cron and Quartz). `* * 15 * MON` fires on every Monday **and** on every 15th of the month.

### Misfire Policies

A *misfire* occurs when a scheduler wakes up and finds a task whose scheduled time has already passed (e.g. the scheduler was stopped and restarted, or the system was under load).

| Policy | Behaviour |
|---|---|
| `IGNORE` | Skip all missed executions, schedule next normally |
| `FIRE_ONCE` | Fire once immediately, then schedule next normally |
| `FIRE_ALL` | Fire once for each missed slot, then schedule next. For `FixedDelayScheduler`, treated as `FIRE_ONCE` since the delay is between executions, not calendar-based |

---

## Result Backend

```kotlin
val backend = RedisResultBackend(factory)

// Store a result (done automatically by the worker)
backend.storeResult(
    taskId,
    TaskResult(
        taskId      = taskId,
        status      = ResultStatus.SUCCESS,
        result      = JsonPrimitive("done"),
        completedAt = Instant.now(),
        worker      = "worker-1"
    )
)

// Fetch
val result = backend.getResult(taskId)

// Block until complete or timeout
val result = backend.awaitResult(taskId, timeout = 30.seconds)
```

### Storing and Fetching Results

Results are stored as JSON at `keyPrefix:result:taskId` with a configurable TTL (default 24h). The worker automatically stores results at each lifecycle stage.

### Result Statuses

| Status | Meaning |
|---|---|
| `PENDING` | Task dispatched, not yet started |
| `STARTED` | Worker picked up the task |
| `SUCCESS` | Completed successfully |
| `FAILURE` | Failed, retries may still occur |
| `REVOKED` | Cancelled before or during execution |
| `REJECTED` | No registered handler for this task name |

---

## Distributed Locking

Kcelery uses Redis-backed distributed locks with:
- **Compare-and-delete** release script (only the lock owner can release)
- **Watchdog renewal** — the lock TTL is kept alive for the entire duration of `block()`, not just the acquisition wait
- **Fencing tokens** — a monotonically increasing counter that callers use to reject writes from stale lock holders

### withLock

```kotlin
val lock = RedisDistributedLock(redisClient)

val result = lock.withLock(
    lockName       = "inventory:update:SKU-42",
    acquireTimeout = 10.seconds,   // how long to retry acquisition
    leaseTtl       = 5.seconds     // TTL per renewal cycle
) { handle ->
    // handle.fencingToken is strictly increasing across all acquisitions
    // of this lock — pass it downstream to reject stale writes
    inventoryService.update(skuId, newQty, fencingToken = handle.fencingToken)
}

if (result == null) {
    // Acquisition timed out — another instance holds the lock
}
```

`acquireTimeout` and `leaseTtl` are independent: `acquireTimeout` controls how long the caller waits to *get* the lock; `leaseTtl` controls how long each Redis TTL window is before the watchdog renews it. `block()` can run longer than `leaseTtl` — the watchdog keeps extending as long as the block is active.

### Fencing Tokens

The fencing token solves the *split-brain write* problem: if a lock holder's JVM pauses (GC, network partition) and the TTL expires, another process acquires the lock and gets a higher token. When the first holder resumes and tries to write, the resource rejects it because its token is lower than the last seen value.

```kotlin
// Resource side — reject writes with stale tokens
fun updateInventory(qty: Int, fencingToken: Long) {
    if (fencingToken <= lastSeenToken) {
        throw StaleWriteException("Token $fencingToken rejected, last seen $lastSeenToken")
    }
    lastSeenToken = fencingToken
    // ... perform write
}
```

### Leader Election

```kotlin
val elector = RedisLeaderElector(redisClient, lock)

// Campaign for leadership
val became = elector.campaign(group = "scheduler", ttl = 30.seconds)

if (became) {
    println("This instance is now leader: ${elector.instanceId}")
    println("Fencing token: ${elector.heldHandles["scheduler"]?.fencingToken}")
}

// React to leadership changes
elector.watchLeadership("scheduler").collect { isLeader ->
    if (isLeader) startScheduler() else stopScheduler()
}

// Register a callback
elector.onPromotion("scheduler") {
    logger.info("Promoted to leader, starting coordinated work")
}

// Voluntarily step down
elector.stepDown("scheduler")
```

Leadership is renewed automatically in the background at `ttl/3` intervals. If the renewal fails (network partition, Redis unavailable), the local state is cleaned up and the lease expires naturally in Redis.

---

## Dead Letter Queue

Messages are dead-lettered when:
- A task exceeds `maxRetries`
- A task throws a non-retryable exception
- The worker explicitly calls `broker.deadLetter(record, reason)`

```kotlin
val dlq = RedisDeadLetterQueue(factory)
```

### Inspecting DLQ Records

```kotlin
// List most recent 50 failures for a specific task
val records = dlq.list(taskName = "process_order", limit = 50)

// Stream all DLQ records without loading into memory
dlq.stream().collect { record ->
    println("${record.task.taskName} failed at ${record.failedAt}: ${record.reason}")
}

// Get a single record
val record = dlq.get("dlq-abc123-1719123456789")

// Stats (breakdowns are from a sample — sampleSize is included in result)
val stats = dlq.getStats(sampleSize = 500)
println("Total failures: ${stats.totalCount}")
println("By task: ${stats.perTask}")
println("By reason: ${stats.perReason}")
```

### Replaying Failed Tasks

```kotlin
// Replay one
dlq.replay("dlq-abc123-...")

// Replay all failures for a task (paginated — won't OOM on large DLQs)
val replayed = dlq.replayByTask("process_order", maxCount = 100)
println("Replayed $replayed tasks")
```

Replay re-enqueues the original `TaskMessage` (with all original args, kwargs, priority, and queue) to the task's declared queue. The DLQ record's `replayCount` and `lastReplayAt` are updated.

### Purging Old Records

```kotlin
// Remove records older than 7 days (batched — safe for large DLQs)
val purged = dlq.purge(olderThan = 7.days)
println("Purged $purged old records")

// Delete all failures for a specific task
dlq.deleteByTask("deprecated_task")
```

DLQ records also have a TTL set via `RedisDeadLetterConfig.retentionPeriod` (default 30 days) so they self-expire even without manual purging.

---

## Metrics

Add `kcelery-metrics` to your dependencies and wire Micrometer:

```kotlin
val metrics = MicrometerTaskMetrics(meterRegistry)

val app = CeleryApp(
    broker  = broker,
    backend = backend,
    metrics = metrics,
    tasks   = listOf(ProcessOrderTask())
)
```

Exposed metrics:

| Metric | Type | Tags |
|---|---|---|
| `kcelery.tasks.processed` | Counter | `task`, `worker`, `status` |
| `kcelery.tasks.execution_time` | Timer | `task`, `worker` |
| `kcelery.tasks.retries` | Counter | `task`, `worker` |
| `kcelery.queue.depth` | Gauge | `queue` |
| `kcelery.dlq.count` | Gauge | — |
| `kcelery.scheduler.misfires` | Counter | `scheduler`, `task` |

---

## Testing

### In-Memory Broker

`kcelery-testkit` provides in-memory implementations of all core interfaces. No Redis, no Docker, instant startup:

```kotlin
class ProcessOrderTaskTest {
    private val broker  = InMemoryMessageBroker()
    private val backend = InMemoryResultBackend()
    private val lock    = InMemoryLock()

    @Test
    fun `task succeeds with valid order`() = runTest {
        val task = ProcessOrderTask()
        val app  = CeleryApp(broker = broker, backend = backend, tasks = listOf(task))

        val msg = app.send(
            taskName = "process_order",
            kwargs   = buildJsonObject { put("orderId", "ORD-1") }
        )

        app.worker("test-worker").processOne()

        val result = backend.getResult(msg.id)
        assertEquals(ResultStatus.SUCCESS, result?.status)
    }
}
```

### Virtual Time and Schedulers

Both `CronScheduler` and `FixedDelayScheduler` accept a `clock: () -> Instant` parameter and use `delay()` for all waiting — making them compatible with `runTest`'s virtual time:

```kotlin
@Test
fun `fixed delay task fires after delay`() = runTest {
    var fired = 0
    val scheduler = FixedDelayScheduler(
        taskExecutor = { _, _ -> fired++ },
        clock        = { Instant.ofEpochMilli(currentTime) } // virtual clock
    )

    scheduler.schedule("test_task", delay = 1.minutes)
    scheduler.start()

    advanceTimeBy(90.seconds)
    assertEquals(1, fired)

    advanceTimeBy(60.seconds)
    assertEquals(2, fired)

    scheduler.stop()
}
```

The key is passing `currentTime` (the `TestCoroutineScheduler`'s virtual milliseconds) into the `clock` lambda. `delay()` calls inside the scheduler advance with `advanceTimeBy` rather than blocking real wall-clock time.

---

## Broker-Agnostic Design

The core interfaces deliberately know nothing about Redis:

```kotlin
// kcelery-core — no Redis imports
interface MessageBroker {
    suspend fun enqueue(task: TaskMessage, queue: String)
    fun consume(queue: String, consumerGroup: String, consumerName: String,
                batchSize: Int, pollTimeout: Duration): Flow<BrokerRecord>
    suspend fun ack(record: BrokerRecord)
    suspend fun nack(record: BrokerRecord, requeue: Boolean)
    suspend fun deadLetter(record: BrokerRecord, reason: String)
    suspend fun scheduleTask(task: TaskMessage)
    suspend fun queueSize(queue: String): Long
    suspend fun purgeQueue(queue: String)
    suspend fun healthCheck(): Boolean
    suspend fun close()
}

interface DistributedLock {
    suspend fun <T> withLock(lockName: String, acquireTimeout: Duration,
                              leaseTtl: Duration, block: suspend (LockHandle) -> T): T?
    suspend fun tryAcquireLeadership(leaderKey: String, ttl: Duration): LockHandle?
    suspend fun release(handle: LockHandle)
}

interface ResultBackend {
    suspend fun storeResult(taskId: String, result: TaskResult)
    suspend fun getResult(taskId: String): TaskResult?
    suspend fun isRevoked(taskId: String): Boolean
    suspend fun close()
}
```

Task code depends only on these interfaces. Swapping Redis for RabbitMQ is a single line change at the composition root.

---

## RabbitMQ Support

`kcelery-rabbitmq` is on the roadmap. It will implement `MessageBroker` using AMQP exchanges and queues with manual ack/nack, and delegate to `kcelery-redis` for `DistributedLock`/`LeaderElector` since RabbitMQ has no native distributed lock primitive.

A mixed configuration will look like:

```kotlin
val app = CeleryApp(
    broker  = RabbitMqMessageBroker(rabbitConfig),   // queuing via RabbitMQ
    lock    = RedisDistributedLock(redisClient),     // locking still via Redis
    backend = RedisResultBackend(redisFactory)       // results still via Redis
)
```

---

## Error Handling Reference

| Scenario | Behaviour |
|---|---|
| Task throws retryable exception | Requeued with backoff up to `maxRetries` |
| Task throws non-retryable exception | Dead-lettered immediately |
| Task exceeds `executionTimeout` | `TimeoutCancellationException` → treated as failure, retried if retryable |
| Worker scope cancelled | `CancellationException` → nacked, not retried, rethrown |
| Lock acquisition timeout | `withLock` returns `null` |
| Lock lost mid-execution (partition) | Watchdog logs warning; `block()` continues but fencing token protects downstream writes |
| Sentinel master failover | Lettuce `MasterReplica` handles transparently; in-flight commands may get a transient error and should be retried at the application level |
| Cluster redirect | Handled automatically by `ClusterClientOptions.maxRedirects` |
| DLQ enqueue failure | Logged as error; original failure still propagates |

---

## Contributing

```bash
git clone https://github.com/vick-ram/kcelery
cd kcelery

# Run all tests
./gradlew test

# Run a specific module's tests
./gradlew :kcelery-redis:test

# Build all modules
./gradlew build
```

Pull requests are welcome. Please:
- Keep new broker implementations in their own module (`kcelery-rabbitmq`, `kcelery-kafka`, etc.)
- Implement the core interfaces in `kcelery-core` — no broker-specific imports in task or scheduler code
- Add corresponding fakes to `kcelery-testkit` for any new backend
- All async code should use `suspend` + `withContext(Dispatchers.IO)` for blocking calls — no `runBlocking`

---

*Kcelery is not affiliated with the Python Celery project.*