package ru.job4j.kafka.reqreply

import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.FutureTask

fun main() {
    repeat(5) {
        val correlationId = UUID.randomUUID().toString()
        val replyService = ReqReplyService()
        val replyMessage = "Send message with correlationId $correlationId"
        val sendTask = FutureTask({
            replyService.sendMessage(correlationId, 1000)
        })
        Thread(sendTask).start()
        Thread({
            Thread.sleep(500)
            replyService.receiveMessage(replyMessage, correlationId)
        }).start()
        println(sendTask.get())
    }
}

class ReqReplyService() {
    private val reqReplyMap: ConcurrentHashMap<String, ReqReply> = ConcurrentHashMap()

    fun sendMessage(correlationId: String, timeout: Long): String {
        //новый ReqReply для каждого запроса, т.е на каждый запрос будет отдельный объект монитор.
        val reqReply = ReqReply(timeout)
        reqReplyMap[correlationId] = reqReply
        return reqReply.send(correlationId).also {
            reqReplyMap.remove(correlationId)
        }
    }

    fun receiveMessage(text: String, correlationId: String) {
        val reqReply = reqReplyMap[correlationId]
        reqReply?.receive(text, correlationId) //Если reqReply равен null, то вызов метода будет пропущен
    }
}