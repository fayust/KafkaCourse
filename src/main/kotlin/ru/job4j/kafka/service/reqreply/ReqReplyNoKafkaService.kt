package ru.job4j.kafka.service.reqreply

import org.springframework.stereotype.Service
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.FutureTask

/**
 * Реализация шаблона ReqReply без Кафки на хранилище ConcurrentHashMap
 */
@Service
class ReqReplyNoKafkaService {
    private val reqReplyMap: ConcurrentHashMap<String, ReqReply> = ConcurrentHashMap()

    fun processReqReply(times: Int) {
        repeat(times) {
            val correlationId = UUID.randomUUID().toString()
            val replyMessage = "Send message with correlationId $correlationId"
            val sendTask = FutureTask({
                sendMessage(correlationId, 1000)
            })
            Thread(sendTask).start()
            Thread({
                Thread.sleep(500)
                receiveMessage(replyMessage, correlationId)
            }).start()
            println(sendTask.get())
        }
    }

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