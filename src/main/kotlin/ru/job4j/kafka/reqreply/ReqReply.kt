package ru.job4j.kafka.reqreply

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
