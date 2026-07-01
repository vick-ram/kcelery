package io.celery.scheduler

/**
 * Policy for handling missed task executions.
 */
enum class MisfirePolicy {
    /** Skip all missed executions, only run future ones */
    IGNORE,

    /** Run once immediately for all missed periods */
    FIRE_ONCE,

    /** Run for each missed period */
    FIRE_ALL
}