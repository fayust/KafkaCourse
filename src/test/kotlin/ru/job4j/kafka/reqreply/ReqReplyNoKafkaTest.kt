package ru.job4j.kafka.reqreply

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import ru.job4j.kafka.service.reqreply.ReqReplyNoKafkaService
import java.util.concurrent.FutureTask

class ReqReplyNoKafkaTest {

    private lateinit var reqReplyService: ReqReplyNoKafkaService

    @BeforeEach
    fun setUp() {
        reqReplyService = ReqReplyNoKafkaService()
    }

    @Test
    fun testSendAndReceiveMatchingCorrelationId() {
        val correlationId = "1"
        val sendTask = FutureTask<String>  {
            reqReplyService.sendMessage(correlationId,1000)
        }
        Thread(sendTask).start()
        Thread {
            Thread.sleep(500)
            reqReplyService.receiveMessage("Send message with correlationId $correlationId", correlationId)
        }.start()
        assertEquals("Send message with correlationId 1", sendTask.get())
    }

    @Test
    fun testSendAndNoReplyAndGetTimeout() {
        val correlationId = "1"
        val sendTask = FutureTask<String> {
            reqReplyService.sendMessage(correlationId, 500)
        }
        Thread(sendTask).start()
        assertEquals("Happened timeout 500", sendTask.get())
    }

    @Test
    fun testSendAndReceiveNonMatchingCorrelationId() {
        val correlationId1 = "1"
        val correlationId2 = "2"
        val sendTask = FutureTask<String>  {
            reqReplyService.sendMessage(correlationId1, 1000)
        }
        Thread(sendTask).start()
        Thread {
            Thread.sleep(500)
            reqReplyService.receiveMessage("Send message with correlationId $correlationId2", correlationId2)
        }.start()
        assertEquals("Happened timeout 1000", sendTask.get())
    }
}