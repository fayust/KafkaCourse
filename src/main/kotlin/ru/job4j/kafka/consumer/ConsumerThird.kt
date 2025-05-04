package ru.job4j.kafka.consumer

import org.apache.kafka.clients.consumer.ConsumerRecords
import ru.job4j.kafka.config.loadConfig
import java.time.Duration

fun main() {
    val config = loadConfig()
    val topic = config.messageTopic
    val thirdConsumerThread = Thread { // Запускаем третьего потребителя
        newConsumer(config).use { consumer ->
            consumer.subscribe(listOf(topic))
            try {
                while (true) {
                    val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofMillis(1000))
                    records.forEach { rec ->
                        println("Third consumer received message: key=${rec.key()}, value=${rec.value()}, partition=${rec.partition()}, offset=${rec.offset()}")
                    }
                }
            } catch (e: Exception) {
                println("Third consumer thread interrupted: ${e.message}")
            }
        }
    }
    thirdConsumerThread.start()

    Runtime.getRuntime().addShutdownHook(Thread {
        println("Stopping consumer 3...")
        thirdConsumerThread.interrupt() // Останавливаем третий поток при остановке приложения.
    })
}