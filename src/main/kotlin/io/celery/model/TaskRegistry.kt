package io.celery.model

import java.util.concurrent.ConcurrentHashMap

class TaskRegistry {
    private val tasks = ConcurrentHashMap<String, CeleryTask<*>>()

    fun register(task: CeleryTask<*>) {
        require(!tasks.containsKey(task.name)) { "Task ${task.name} already registered" }
        tasks[task.name] = task
    }

    fun getTask(taskName: String): CeleryTask<*>? = tasks[taskName]
}