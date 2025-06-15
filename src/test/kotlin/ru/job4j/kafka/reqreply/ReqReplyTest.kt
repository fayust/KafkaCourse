package ru.job4j.kafka.reqreply

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import ru.job4j.kafka.service.reqreply.ReqReplyService
import java.util.concurrent.FutureTask

class ReqReplyTest {

    @Test
    fun testSendAndReceiveMatchingCorrelationId() {
        val correlationId = "1"
        val replyService = ReqReplyService()
        val sendTask = FutureTask<String>  {
            replyService.sendMessage(correlationId,1000)
        }
        Thread(sendTask).start()
        Thread {
            Thread.sleep(500)
            replyService.receiveMessage("Send message with correlationId $correlationId", correlationId)
        }.start()
        assertEquals("Send message with correlationId 1", sendTask.get())
    }

    @Test
    fun testSendAndNoReplyAndGetTimeout() {
        val correlationId = "1"
        val replyService = ReqReplyService()
        val sendTask = FutureTask<String> {
            replyService.sendMessage(correlationId, 500)
        }
        Thread(sendTask).start()
        assertEquals("Happened timeout 500", sendTask.get())
    }

    @Test
    fun testSendAndReceiveNonMatchingCorrelationId() {
        val correlationId1 = "1"
        val correlationId2 = "2"
        val replyService = ReqReplyService()
        val sendTask = FutureTask<String>  {
            replyService.sendMessage(correlationId1, 1000)
        }
        Thread(sendTask).start()
        Thread {
            Thread.sleep(500)
            replyService.receiveMessage("Send message with correlationId $correlationId2", correlationId2)
        }.start()
        assertEquals("Happened timeout 1000", sendTask.get())
    }
}