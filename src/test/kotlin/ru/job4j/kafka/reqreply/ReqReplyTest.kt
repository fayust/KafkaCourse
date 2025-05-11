package ru.job4j.kafka.reqreply

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.concurrent.FutureTask

class ReqReplyTest {

    @Test
    fun testSendAndReceiveMatchingCorrelationId() {
        val correlationId = "1"
        val reply = ReqReply(1000)
        val sendTask = FutureTask<String>  {
            reply.send(correlationId)
        }
        Thread(sendTask).start()
        Thread {
            Thread.sleep(500)
            reply.receive("Send message with correlationId $correlationId", correlationId)
        }.start()
        assertEquals("Send message with correlationId 1", sendTask.get())
    }

    @Test
    fun testSendAndNoReplyAndGetTimeout() {
        val correlationId = "1"
        val reply = ReqReply(500)
        val sendTask = FutureTask<String> {
            reply.send(correlationId)
        }
        Thread(sendTask).start()
        assertEquals("Happened timeout 500", sendTask.get())
    }

    @Test
    fun testSendAndReceiveNonMatchingCorrelationId() {
        val correlationId1 = "1"
        val correlationId2 = "2"
        val reply = ReqReply(1000)
        val sendTask = FutureTask<String>  {
            reply.send(correlationId1)
        }
        Thread(sendTask).start()
        Thread {
            Thread.sleep(500)
            reply.receive("Send message with correlationId $correlationId2", correlationId2)
        }.start()
        assertEquals("Happened timeout 1000", sendTask.get())
    }
}