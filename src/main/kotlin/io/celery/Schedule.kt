package io.celery

sealed class Schedule {
    data class Timedelta(val seconds: Long) : Schedule()
    data class Crontab(val expression: String) : Schedule()

    companion object {
        fun every(seconds: Long) = Timedelta(seconds)
        fun crontab(expression: String) = Crontab(expression)
    }
}