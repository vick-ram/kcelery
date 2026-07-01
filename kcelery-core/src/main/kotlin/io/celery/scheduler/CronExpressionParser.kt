package io.celery.scheduler

@Serializable
data class CronExpressionParser(
    val seconds: Set<Int> = setOf(0),
    val minutes: Set<Int> = setOf(0),
    val hours: Set<Int> = setOf(0),
    val daysOfMonth: Set<Int> = (1..31).toSet(),
    val months: Set<Int> = (1..12).toSet(),
    val daysOfWeek: Set<DayOfWeek> = DayOfWeek.entries.toSet()
) {
    fun matches(dateTime: ZonedDateTime): Boolean {
        return dateTime.second in seconds &&
                dateTime.minute in minutes &&
                dateTime.hour in hours &&
                dateTime.dayOfMonth in daysOfMonth &&
                dateTime.monthValue in months &&
                dateTime.dayOfWeek in daysOfWeek
    }

    fun nextMatchAfter(dateTime: ZonedDateTime, maxAttempts: Int = 366 * 24 * 60 * 60): ZonedDateTime {
        var current = dateTime.withNano(0).plusSeconds(1)
        var attempts = 0

        while (attempts < maxAttempts) {
            // Quick skip for invalid months
            if (!months.contains(current.monthValue)) {
                current = skipToNextMonth(current)
                attempts++
                continue
            }

            // Quick skip for invalid days
            if (!isDayValid(current)) {
                current = current.plusDays(1).withHour(0).withMinute(0).withSecond(0).withNano(0)
                attempts++
                continue
            }

            // Quick skip for invalid hours
            if (!hours.contains(current.hour)) {
                current = skipToNextHour(current)
                attempts++
                continue
            }

            // Quick skip for invalid minutes
            if (!minutes.contains(current.minute)) {
                current = skipToNextMinute(current)
                attempts++
                continue
            }

            // Quick skip for invalid seconds
            if (!seconds.contains(current.second)) {
                current = skipToNextSecond(current)
                attempts++
                continue
            }

            return current
        }

        throw IllegalStateException("Unable to find next match within $maxAttempts attempts")
    }

    private fun isDayValid(dateTime: ZonedDateTime): Boolean {
        return daysOfMonth.contains(dateTime.dayOfMonth) &&
                daysOfWeek.contains(dateTime.dayOfWeek)
    }

    private fun skipToNextMonth(current: ZonedDateTime): ZonedDateTime {
        val currentMonth = current.monthValue
        val sortedMonths = months.sorted()

        val nextMonth = sortedMonths.firstOrNull { it > currentMonth }
            ?: sortedMonths.first()

        val yearAdjust = if (nextMonth <= currentMonth) 1 else 0
        return current
            .plusYears(yearAdjust.toLong())
            .withMonth(nextMonth)
            .withDayOfMonth(1)
            .withHour(0)
            .withMinute(0)
            .withSecond(0)
            .withNano(0)
    }

    private fun skipToNextHour(current: ZonedDateTime): ZonedDateTime {
        val nextHour = hours.filter { it > current.hour }.minOrNull()
        return if (nextHour != null) {
            current.withHour(nextHour).withMinute(0).withSecond(0).withNano(0)
        } else {
            current.plusDays(1).withHour(hours.min()).withMinute(0).withSecond(0).withNano(0)
        }
    }

    private fun skipToNextMinute(current: ZonedDateTime): ZonedDateTime {
        val nextMinute = minutes.filter { it > current.minute }.minOrNull()
        return if (nextMinute != null) {
            current.withMinute(nextMinute).withSecond(0).withNano(0)
        } else {
            current.plusHours(1).withMinute(minutes.min()).withSecond(0).withNano(0)
        }
    }

    private fun skipToNextSecond(current: ZonedDateTime): ZonedDateTime {
        val nextSecond = seconds.filter { it > current.second }.minOrNull()
        return if (nextSecond != null) {
            current.withSecond(nextSecond).withNano(0)
        } else {
            current.plusMinutes(1).withSecond(seconds.min())
        }
    }

    companion object {
        fun parse(expression: String): CronExpressionParser {
            val parts = expression.trim().split("\\s+".toRegex())

            require(parts.size in 5..6) {
                "Invalid cron expression: '$expression'. Expected 5 or 6 fields"
            }

            val offset = if (parts.size == 6) 0 else -1

            return CronExpressionParser(
                seconds = if (offset == 0) parseField(parts[0], 0..59, "seconds") else setOf(0),
                minutes = parseField(parts[1 + offset], 0..59, "minutes"),
                hours = parseField(parts[2 + offset], 0..23, "hours"),
                daysOfMonth = parseField(parts[3 + offset], 1..31, "days of month"),
                months = parseField(parts[4 + offset], 1..12, "months"),
                daysOfWeek = parseDayOfWeekField(parts[5 + offset])
            )
        }

        private fun parseField(field: String, range: IntRange, fieldName: String): Set<Int> {
            if (field == "*" || field == "?") return range.toSet()
            if (field == "L") return setOf(range.last)

            val values = mutableSetOf<Int>()
            val stepRanges = mutableListOf<Pair<IntRange, Int>>()

            field.split(",").forEach { part ->
                val (rangePart, step) = if (part.contains("/")) {
                    val split = part.split("/")
                    split[0] to split[1].toIntOrNull()
                } else {
                    part to null
                }

                if (rangePart == "*") {
                    stepRanges.add(range to (step ?: 1))
                } else if (rangePart.contains("-")) {
                    val (start, end) = rangePart.split("-").map { it.toInt() }
                    require(start <= end) { "Invalid range in $fieldName: $start-$end" }
                    stepRanges.add((start..end) to (step ?: 1))
                } else if (rangePart.contains("L")) {
                    step?.let { values.add(range.last) }
                } else {
                    val num = rangePart.toIntOrNull()
                        ?: throw IllegalArgumentException("Invalid number in $fieldName: $rangePart")
                    require(num in range) { "Value $num out of range $range for $fieldName" }
                    stepRanges.add((num..range.last) to (step ?: range.last))
                }
            }

            stepRanges.forEach { (r, step) ->
                for (i in r.first..r.last step step) {
                    if (i in range) values.add(i)
                }
            }

            require(values.isNotEmpty()) { "No valid values for $fieldName" }
            return values
        }

        private fun parseDayOfWeekField(field: String): Set<DayOfWeek> {
            if (field == "*" || field == "?") return DayOfWeek.entries.toSet()

            val dayMap = mapOf(
                "SUN" to DayOfWeek.SUNDAY, "MON" to DayOfWeek.MONDAY,
                "TUE" to DayOfWeek.TUESDAY, "WED" to DayOfWeek.WEDNESDAY,
                "THU" to DayOfWeek.THURSDAY, "FRI" to DayOfWeek.FRIDAY,
                "SAT" to DayOfWeek.SATURDAY,
                "0" to DayOfWeek.SUNDAY, "1" to DayOfWeek.MONDAY,
                "2" to DayOfWeek.TUESDAY, "3" to DayOfWeek.WEDNESDAY,
                "4" to DayOfWeek.THURSDAY, "5" to DayOfWeek.FRIDAY,
                "6" to DayOfWeek.SATURDAY, "7" to DayOfWeek.SUNDAY
            )

            val values = mutableSetOf<DayOfWeek>()

            field.split(",").forEach { part ->
                when {
                    part.contains("-") -> {
                        val (start, end) = part.split("-")
                        val startDay = parseDayValue(start.trim(), dayMap)
                        val endDay = parseDayValue(end.trim(), dayMap)
                        var current = startDay
                        while (current != endDay) {
                            values.add(current)
                            current = current.plus(1)
                        }
                        values.add(endDay)
                    }
                    part.contains("/") -> {
                        val (rangePart, stepStr) = part.split("/")
                        val step = stepStr.toInt()
                        val startDay = if (rangePart == "*") DayOfWeek.MONDAY
                        else parseDayValue(rangePart, dayMap)
                        var current = startDay
                        repeat(7 / step) {
                            values.add(current)
                            current = current.plus(step.toLong())
                        }
                    }
                    else -> {
                        values.add(parseDayValue(part.trim(), dayMap))
                    }
                }
            }

            return values.ifEmpty { DayOfWeek.entries.toSet() }
        }

        private fun parseDayValue(value: String, dayMap: Map<String, DayOfWeek>): DayOfWeek {
            return value.toIntOrNull()?.let {
                DayOfWeek.of(if (it == 0) 7 else it)
            } ?: dayMap[value.uppercase()]
            ?: throw IllegalArgumentException("Invalid day of week: $value")
        }
    }
}