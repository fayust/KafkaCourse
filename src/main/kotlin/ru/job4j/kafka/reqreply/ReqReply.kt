package ru.job4j.kafka.reqreply

import java.util.*
import java.util.concurrent.FutureTask

class ReqReply(val timeout: Long) {
    private val monitor = Object()
    private var message = ""
    private var received_correlationId = ""
    private var received = false

    fun send(correlationId: String): String {
        synchronized(monitor) {
            if (!received) {
                try {
                    monitor.wait(timeout)
                } catch (e: InterruptedException) {
                    Thread.currentThread().interrupt()
                    return "Interrupted while waiting"
                }
            }
            return if (received && received_correlationId == correlationId) {
                    message
            } else "Happened timeout $timeout"
        }
    }

    fun receive(text: String, correlationId: String) {
        synchronized(monitor) {
            received = true
            message = text
            received_correlationId = correlationId
            monitor.notifyAll()
        }
    }
}

fun main() {
    val correlationId = UUID.randomUUID().toString()
    val reply = ReqReply(1000)
    val sendTask = FutureTask({
        reply.send(correlationId)
    })
    Thread(sendTask).start()
    Thread({
        Thread.sleep(500)
        reply.receive("Send message with correlationId " + correlationId, correlationId)
    }).start()
    println(sendTask.get())
}