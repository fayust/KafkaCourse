package ru.job4j.kafka.consumer

import org.apache.kafka.clients.consumer.ConsumerRecords
import ru.job4j.kafka.config.loadConfig
import java.time.Duration

fun main() {
    val config = loadConfig()
    val topic = config.taskTopic
    // Запускаем второго потребителя
    val secondConsumerThread = Thread {
        newConsumer(config).use { consumer ->
            consumer.subscribe(listOf(topic))
            try {
                while (true) {
                    val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofMillis(1000))
                    records.forEach { rec ->
                        println("Second consumer received message: key=${rec.key()}, value=${rec.value()}, partition=${rec.partition()}, offset=${rec.offset()}")
                    }
                }
            } catch (e: Exception) {
                println("Second consumer thread interrupted: ${e.message}")
            }
        }
    }
    secondConsumerThread.start()

    Runtime.getRuntime().addShutdownHook(Thread {
        println("Stopping consumer 2...")
        secondConsumerThread.interrupt() // Останавливаем второй поток при остановке приложения.
    })
}