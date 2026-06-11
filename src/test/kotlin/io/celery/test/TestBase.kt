package io.celery.test

import io.celery.core.Clock
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension
import java.time.Instant
import java.time.ZoneOffset

@ExperimentalCoroutinesApi
@ExtendWith(MockitoExtension::class)
abstract class TestBase {

    protected val testDispatcher = UnconfinedTestDispatcher()
    protected val testScope = TestScope(testDispatcher)

    protected val json = Json {
        ignoreUnknownKeys = true
        prettyPrint = false
        isLenient = true
    }

    protected lateinit var clock: Clock

    @BeforeEach
    open fun setUp() {
        clock = Clock.fixed(
            Instant.parse("2024-01-15T10:00:00Z"),
            ZoneOffset.UTC
        )
    }

    @AfterEach
    open fun tearDown() {
        // Cleanup if needed
    }

    protected fun runTest(block: suspend TestScope.() -> Unit) =
        kotlinx.coroutines.test.runTest(testDispatcher) {
            block()
        }
}