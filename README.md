# KCelery - Kotlin Distributed Task Queue & Scheduler

[![Kotlin](https://img.shields.io/badge/Kotlin-1.9%252B-blue.svg)](https://kotlinlang.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)](https://github.com/yourorg/kcelery/actions)

KCelery is a production-ready, distributed task queue and scheduler for Kotlin, inspired by Python's Celery but built natively for the JVM ecosystem. It combines powerful scheduling capabilities with reliable async task execution using Redis.

## 🌟 Features

### 🎯 Core Capabilities

*   **Distributed Task Execution**: Execute tasks across multiple workers with automatic load balancing.
*   **Cron-based Scheduling**: Full cron expression support with 5 or 6 field expressions.
*   **Multiple Trigger Types**: Cron, fixed-delay, and fixed-rate scheduling.
*   **Async Task Dispatching**: Fire-and-forget task execution with result tracking.
*   **Message Broker**: Redis Streams-based message brokering with consumer groups.
*   **Result Backend**: Persistent result storage with configurable TTL.

### 🔧 Advanced Features

*   **Automatic Retries**: Configurable retry policies with exponential backoff.
*   **Misfire Handling**: Three policies (IGNORE, FIRE_ONCE, FIRE_ALL).
*   **Dead Letter Queue**: Capture and analyze failed tasks.
*   **Concurrency Control**: Per-task and global concurrency limits.
*   **Task Expiration**: Automatic cleanup of stale tasks.
*   **Graceful Shutdown**: Proper cleanup and state persistence.
*   **Leader Election**: Prevent duplicate execution in distributed environments.

### 📊 Production Ready

*   **Persistence**: All state persisted in Redis for crash recovery.
*   **Monitoring**: Built-in metrics via Micrometer/Prometheus.
*   **Distributed Locks**: Redis-based locking prevents race conditions.
*   **Health Checks**: Worker heartbeat and scheduler health monitoring.
*   **Error Handling**: Comprehensive error handling with structured logging.
*   **Backpressure**: Channel-based execution with bounded queues.

## 📦 Installation

### Gradle

```kotlin
dependencies {
    implementation("io.github.vickram:kcelery-core:1.0.0")
    implementation("io.github.vickram:kcelery-redis:1.0.0")
    implementation("io.github.vickram:kcelery-metrics:1.0.0")
}
```

### Maven

```xml
<dependency>
    <groupId>io.github.vickram</groupId>
    <artifactId>kcelery-core</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Requirements

*   Kotlin 1.9+
*   Redis 6.0+ (with Streams support)
*   JVM 21+

## 🚀 Quick Start

### 1. Define Your Tasks

```kotlin
import io.celery.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.*

@Serializable
data class EmailResult(val success: Boolean, val messageId: String)

// Create a unified task (works for both scheduling and async execution)
val emailTask = object : CeleryTask<EmailResult>(
    name = "send-email",
    maxRetries = 3,
    defaultRetryDelay = 60,
    serializer = EmailResult.serializer()
) {
    override suspend fun run(context: TaskContext): EmailResult {
        val to = context.kwargs["to"]?.jsonPrimitive?.content
            ?: throw IllegalArgumentException("Missing recipient")
        val subject = context.kwargs["subject"]?.jsonPrimitive?.content ?: "No Subject"

        // Your email sending logic here
        // sendEmail(to, subject, context.kwargs["body"]?.jsonPrimitive?.content)
        
        return EmailResult(true, "msg-${context.taskId}")
    }
    
    override fun onRetry(exc: Exception, retries: Int): Long {
        return 60 * (1L shl (retries - 1)) // Exponential backoff
    }
}
```

### 2. Create Application

```kotlin
import io.celery.CeleryApp
import kotlinx.serialization.json.JsonPrimitive

suspend fun main() {
    val app = CeleryApp(
        name = "my-app",
        redisUrl = "redis://localhost:6379"
    )

    // Register tasks
    app.registerTask(emailTask)
    
    // Start workers and scheduler
    app.start(workerCount = 4, workerConcurrency = 10)
    
    // Schedule recurring task - every 5 minutes
    app.scheduleCron(
        id = "periodic-report",
        taskName = "send-email",
        cronExpression = "0 */5 * * * *",
        kwargs = mapOf(
            "to" to JsonPrimitive("admin@example.com"),
            "subject" to JsonPrimitive("Periodic Report")
        )
    )
    
    // Send async task immediately
    val result = app.sendTask(
        taskName = "send-email",
        kwargs = mapOf(
            "to" to JsonPrimitive("user@example.com"),
            "subject" to JsonPrimitive("Welcome!")
        ),
        priority = 1
    )
}
```

## 📚 Core Concepts

### Task Types

KCelery supports different ways to execute tasks:

```kotlin
// 1. Async Task - Execute once, immediately or with delay
app.sendTask(
    taskName = "process-data",
    args = listOf(JsonPrimitive("data.csv")),
    countdown = 300, // Execute in 5 minutes
    priority = 2
)

// 2. Cron Task - Execute on schedule
app.scheduleCron(
    id = "daily-backup",
    taskName = "backup-database",
    cronExpression = "0 0 2 * * *" // Every day at 2 AM
)

// 3. Fixed Delay Task - Execute with constant delay between completions
app.scheduleFixedDelay(
    id = "health-check",
    taskName = "check-health",
    delayMs = 30_000 // Every 30 seconds
)

// 4. Fixed Rate Task - Execute at constant rate
app.scheduleFixedRate(
    id = "metrics-collect",
    taskName = "collect-metrics",
    periodMs = 60_000 // Every minute
)
```

### Cron Expression Syntax

KCelery supports both 5 and 6 field cron expressions:

```text
┌────────── second (0-59)
│ ┌────────── minute (0-59)
│ │ ┌────────── hour (0-23)
│ │ │ ┌────────── day of month (1-31)
│ │ │ │ ┌────────── month (1-12)
│ │ │ │ │ ┌────────── day of week (0-7 or SUN-SAT)
│ │ │ │ │ │
* * * * * *
```

**Examples:**

```kotlin
// Every 5 minutes
"0 */5 * * * *"

// Every weekday at 9 AM
"0 0 9 * * MON-FRI"

// First day of every month at midnight
"0 0 0 1 * *"

// Every 15 seconds
"*/15 * * * * *"
```

**Special Characters:**

*   `*` - Any value
*   `,` - Value list separator (e.g., `1,3,5`)
*   `-` - Range of values (e.g., `1-5`)
*   `/` - Step values (e.g., `*/15` for every 15 units)
*   `?` - No specific value (for day of month/week)
*   `L` - Last day of month/week

### Task Configuration

```kotlin
import io.celery.core.TaskConfig
import io.celery.core.MisfirePolicy

val config = TaskConfig(
    allowConcurrentExecution = false,  // Prevent overlapping executions
    misfirePolicy = MisfirePolicy.FIRE_ONCE, // Handle missed executions
    maxRetries = 3,                   // Maximum retry attempts
    retryDelayMs = 1000,              // Initial retry delay
    maxRetryDelayMs = 60_000,         // Maximum retry delay
    retryBackoffMultiplier = 2.0,     // Exponential backoff factor
    timeoutMs = 30_000,               // Task timeout (30 seconds)
    deadLetterEnabled = true          // Enable dead letter queue
)
```

### Retry Policies

```kotlin
class RetryExampleTask : CeleryTask<String>(
    name = "retry-example",
    maxRetries = 5
) {
    override suspend fun run(context: TaskContext): String {
        // Your logic here
        return "success"
    }

    override fun onRetry(exc: Exception, retries: Int): Long {
        // Custom retry logic
        return when (exc) {
            // is NetworkException -> 60 * retries.toLong() // Linear backoff
            // is RateLimitException -> 300 // Fixed delay for rate limits
            else -> defaultRetryDelay * (1L shl (retries - 1)) // Exponential
        }
    }
    
    override fun onFailure(exc: Exception) {
        // Called when all retries are exhausted
        // monitoringService.alert("Task failed permanently: $name", exc)
    }
}
```

### Misfire Handling

```kotlin
// IGNORE: Skip missed executions (default)
MisfirePolicy.IGNORE

// FIRE_ONCE: Execute once for all missed periods
MisfirePolicy.FIRE_ONCE

// FIRE_ALL: Execute for each missed period
MisfirePolicy.FIRE_ALL
```

## 🏗️ Architecture

### System Overview

```text
┌─────────────────────────────────────────────────────────────┐
│                      KCelery Application                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌──────────────────┐         ┌──────────────────┐          │
│  │   Scheduler      │         │   Task Producer  │          │
│  │  (Cron/Fixed)    │         │  (Async Tasks)   │          │
│  └────────┬─────────┘         └────────┬─────────┘          │
│           │                            │                    │
│           └──────────┬─────────────────┘                    │
│                      │                                      │
│              ┌───────▼────────┐                             │
│              │  Redis Streams │                             │
│              │  (Message Queue)│                            │
│              └───────┬────────┘                             │
│                      │                                      │
│           ┌──────────┼──────────┐                           │
│           │          │          │                           │
│    ┌──────▼──┐ ┌────▼───┐ ┌───-▼─────┐                      │
│    │ Worker 1│ │Worker 2│ │Worker N  │                      │
│    └──────┬──┘ └────┬───┘ └───┬──────┘                      │
│           │         │         │                             │
│           └─────────┼─────────┘                             │
│                     │                                       │
│            ┌────────▼────────┐                              │
│            │  Result Backend │                              │
│            │     (Redis)     │                              │
│            └─────────────────┘                              │
└─────────────────────────────────────────────────────────────┘
```

### Component Interactions

```text
┌─────────────┐
│ Task Creator │
└──────┬──────┘
       │
       ├──► Scheduler ──► Task Queue ──► Execution Engine
       │        │                              │
       │        └──► Redis Persistence         │
       │                                       │
       └──► Message Broker ──► Workers ──► Results
                               │
                               └──► Dead Letter Queue
```

## 🔧 Configuration

### Application Configuration

```kotlin
val app = CeleryApp(
    name = "production-app",
    redisUrl = "redis://redis-cluster:6379",
    workerThreads = 8
)
```

### Redis Configuration

```yaml
# For high availability with Redis Sentinel
redis:
  sentinel:
    master: mymaster
    nodes:
      - sentinel1:26379
      - sentinel2:26379
      - sentinel3:26379

# For Redis Cluster
redis:
  cluster:
    nodes:
      - redis1:6379
      - redis2:6379
      - redis3:6379
```

### Worker Configuration

```kotlin
// Start with custom worker settings
app.start(
    workerCount = 4,          // Number of worker instances
    workerConcurrency = 10,   // Concurrent tasks per worker
    queues = listOf(
        "high-priority",
        "default",
        "low-priority"
    )
)
```

## 📊 Monitoring & Metrics

### Built-in Metrics

KCelery exposes metrics via Micrometer:

```kotlin
// Available metrics
celery.scheduler.queue.size          // Current queue depth
celery.scheduler.executions.total    // Total executions
celery.scheduler.failures.total      // Total failures
celery.scheduler.skips.total         // Skipped executions
celery.scheduler.retries.total       // Retry attempts
celery.task.execution                // Task execution timer
celery.task.failure                  // Task failure counter
celery.task.skip                     // Task skip counter
celery.task.retry                    // Task retry counter
```

### Prometheus Integration

```kotlin
import io.micrometer.prometheus.PrometheusMeterRegistry

// Metrics are automatically exposed
// Access via /actuator/prometheus endpoint
```

### Custom Metrics

```kotlin
class MonitoredTask : CeleryTask<String>("monitored-task") {
    private val executionTimer = metrics.timer("custom.task.duration")

    override suspend fun run(context: TaskContext): String {
        return executionTimer.record {
            // Your task logic
            "success"
        }
    }
}
```

## 🔒 Distributed Execution

### Leader Election

KCelery automatically handles leader election to prevent duplicate scheduling:

```kotlin
// Multiple instances can run simultaneously
// Only the leader will execute scheduled tasks

// Instance 1: Started (Leader) ✓
// Instance 2: Started (Standby)
// Instance 3: Started (Standby)

// If leader fails, a standby takes over automatically
// Instance 1: Failed ✗
// Instance 2: Started (Leader) ✓  ← Promoted
// Instance 3: Started (Standby)
```

### Distributed Locks

```kotlin
// Automatic lock management prevents race conditions
class CriticalTask : CeleryTask<Unit>("critical-update") {
    override suspend fun run(context: TaskExecutionContext) {
        // Only one instance of this task runs at a time
        // (when allowConcurrentExecution = false)
        // performAtomicUpdate()
    }
}
```

## 🛡️ Error Handling

### Dead Letter Queue

Failed tasks are automatically moved to a dead letter queue:

```kotlin
// Monitor dead letters
suspend fun monitorDeadLetters() {
    // val deadLetters = redis.xrange("celery:dead:*", "-", "+")
    // deadLetters.forEach { (id, fields) ->
    //     val task = json.decodeFromString<TaskMessage>(fields["task"]!!)
    //     val reason = fields["reason"]
    //     println("Dead letter: ${task.id} - Reason: $reason")
    // }
}
```

### Retry Strategies

```kotlin
// Fixed delay
TaskConfig(retryDelayMs = 5000)

// Linear backoff (5s, 10s, 15s, ...)
override fun onRetry(exc: Exception, retries: Int) =
    defaultRetryDelay * retries.toLong()

// Exponential backoff (60s, 120s, 240s, ...)
override fun onRetry(exc: Exception, retries: Int) =
    defaultRetryDelay * (1L shl (retries - 1))

// Fibonacci backoff (60s, 60s, 120s, 180s, 300s, ...)
override fun onRetry(exc: Exception, retries: Int): Long {
    if (retries <= 1) return 60
    var a = 60L; var b = 60L
    repeat(retries - 1) { val c = a + b; a = b; b = c }
    return b
}
```

## 🚢 Production Deployment

### Docker

```dockerfile
FROM openjdk:17-jdk-slim

COPY build/libs/kcelery-app.jar /app/
COPY config/application.yml /app/config/

WORKDIR /app
CMD ["java", "-jar", "kcelery-app.jar"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data

  worker-1:
    build: .
    environment:
      - REDIS_URL=redis://redis:6379
      - WORKER_NAME=worker-1
    depends_on:
      - redis

  worker-2:
    build: .
    environment:
      - REDIS_URL=redis://redis:6379
      - WORKER_NAME=worker-2
    depends_on:
      - redis

volumes:
  redis-data:
```

### Kubernetes

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kcelery-worker
spec:
  replicas: 3
  selector:
    matchLabels:
      app: kcelery-worker
  template:
    metadata:
      labels:
        app: kcelery-worker
    spec:
      containers:
        - name: worker
          image: kcelery:latest
          env:
            - name: REDIS_URL
              value: "redis://redis-service:6379"
            - name: WORKER_THREADS
              value: "4"
          resources:
            requests:
              memory: "512Mi"
              cpu: "500m"
            limits:
              memory: "1Gi"
              cpu: "1000m"
---
apiVersion: v1
kind: Service
metadata:
  name: redis-service
spec:
  selector:
    app: redis
  ports:
    - port: 6379
```

## 🔍 Troubleshooting

### Common Issues

1.  **Tasks not executing**
    ```kotlin
    // Check if workers are running
    // val stats = app.getStats()
    // println("Active workers: ${stats.workers}")

    // Check for dead letters
    // app.inspectDeadLetters()
    ```
2.  **Duplicate executions**
    ```kotlin
    // Ensure leader election is working
    // Check Redis for multiple leaders
    // redis.keys("celery:leader:*")
    ```
3.  **Memory issues**
    ```kotlin
    // Monitor queue sizes
    // metrics.gauge("celery.queue.size").value()

    // Clean up old results
    // app.cleanupCompletedTasks(olderThan = 7.days)
    ```

### Debug Logging

```kotlin
// Enable debug logging
// logging:
//   level:
//     io.celery: DEBUG
//     org.redisson: INFO
```

## 📈 Performance Tuning

```kotlin
// Optimize for throughput
val app = CeleryApp(
    workerThreads = Runtime.getRuntime().availableProcessors() * 2,
    workerConcurrency = 20
)

// Optimize for memory
TaskConfig(
    deadLetterEnabled = false,  // Disable dead letters
    timeoutMs = 10_000          // Aggressive timeouts
)

// Batch processing
app.scheduleCron(
    id = "batch-process",
    taskName = "process-batch",
    cronExpression = "0 */1 * * * *",
    config = TaskConfig(
        allowConcurrentExecution = false  // Prevent overlapping batches
    )
)
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md).

### Development Setup

```bash
# Clone the repository
git clone https://github.com/vickram/kcelery.git

# Build the project
./gradlew build

# Run tests
./gradlew test

# Start Redis for development
docker run -d -p 6379:6379 redis:7-alpine
```

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

*   Inspired by Python's Celery
*   Built with Kotlin Coroutines
*   Uses Lettuce for Redis connectivity
*   Metrics via Micrometer

## 🔗 Links

*   [Documentation](https://vickram.github.io/kcelery/docs)
*   [Examples](https://github.com/vickram/kcelery/tree/main/examples)
*   [API Reference](https://vickram.github.io/kcelery/api)
*   [Changelog](https://github.com/vickram/kcelery/releases)
*   [Migration Guide](https://vickram.github.io/kcelery/migration)

KCelery - Reliable, scalable task scheduling and execution for Kotlin applications.
