package ru.job4j.kafka.reqreply

import io.mockk.*
import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.api.TestInstance
import java.util.UUID
import java.util.concurrent.FutureTask

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReqReplyBaseRockGeneratedTest {

    private lateinit var reqReply: ReqReply

    @BeforeEach
    fun setUp() {
        reqReply = ReqReply(1000)
    }

    @AfterEach
    fun tearDown() {
        clearAllMocks()
    }

    @Test
    @Timeout(10)
    fun testSendWithoutReceive() {
        val correlationId = UUID.randomUUID().toString()
        val result = reqReply.send(correlationId)
        assertThat(result, equalTo("Happened timeout 1000"))
    }

    @Test
    @Timeout(10)
    fun testSendWithReceive() {
        val correlationId = UUID.randomUUID().toString()
        val message = "Test message"
        
        Thread {
            Thread.sleep(500)
            reqReply.receive(message, correlationId)
        }.start()

        val result = reqReply.send(correlationId)
        assertThat(result, equalTo(message))
    }

    @Test
    @Timeout(10)
    fun testSendWithInterruption() {
        val correlationId = UUID.randomUUID().toString()
        
        val thread = Thread {
            reqReply.send(correlationId)
        }
        thread.start()
        Thread.sleep(100) // Give some time for the thread to enter the wait state
        thread.interrupt()
        thread.join()

        val result = reqReply.send(correlationId)
        assertThat(result, equalTo("Happened timeout 1000"))
    }

    @Test
    @Timeout(10)
    fun testReceiveBeforeSend() {
        val correlationId = UUID.randomUUID().toString()
        val message = "Test message"

        reqReply.receive(message, correlationId)
        val result = reqReply.send(correlationId)

        assertThat(result, equalTo(message))
    }

    @ParameterizedTest
    @MethodSource("provideCorrelationIds")
    @Timeout(10)
    fun testSendWithDifferentCorrelationIds(correlationId: String) {
        val message = "Test message"
        reqReply.receive(message, "different-id")
        val result = reqReply.send(correlationId)
        assertThat(result, equalTo("Happened timeout 1000"))
    }

    private fun provideCorrelationIds() = listOf(
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString(),
        UUID.randomUUID().toString()
    )

    @Test
    @Timeout(10)
    fun testMainFunction() {
        mockkStatic(UUID::class)
        val mockUuid = UUID.randomUUID()
        every { UUID.randomUUID() } returns mockUuid

        mockkConstructor(ReqReply::class)
        every { anyConstructed<ReqReply>().send(any()) } returns "Mocked response"

        mockkConstructor(FutureTask::class)
        every { anyConstructed<FutureTask<String>>().get() } returns "Mocked response"

        main()

        verify {
            UUID.randomUUID()
            anyConstructed<ReqReply>().send(mockUuid.toString())
            anyConstructed<FutureTask<String>>().get()
        }
    }
}