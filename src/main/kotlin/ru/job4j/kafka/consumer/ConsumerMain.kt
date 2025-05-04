package ru.job4j.kafka.consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import ru.job4j.kafka.config.KafkaConfig
import ru.job4j.kafka.config.loadConfig
import java.time.Duration
import java.util.*

fun main() {
    val config = loadConfig()
    val topic = config.messageTopic
    val firstConsumerThread = Thread { // Запускаем первого потребителя
        newConsumer(config).use { consumer ->
            consumer.subscribe(listOf(topic))
            try {
                while (true) {
                    consumer.poll(Duration.ofMillis(1000)).forEach { rec ->
                        println("First consumer received message: key=${rec.key()}, value=${rec.value()}, partition=${rec.partition()}, offset=${rec.offset()}")
                    }
                }
            } catch (e: Exception) {
                println("First consumer thread interrupted: ${e.message}")
            }
        }
    }

    firstConsumerThread.start()

    Runtime.getRuntime().addShutdownHook(Thread {
        println("Stopping consumer 1...")
        firstConsumerThread.interrupt() // Останавливаем первый поток при остановке приложения.
    })
}


fun newConsumer(config: KafkaConfig): KafkaConsumer<String, String> {
    val consumerConfig = Properties().apply {
        put("bootstrap.servers", config.bootstrapServers)
        put("key.deserializer", config.consumer.keyDeserializer)
        put("value.deserializer", config.consumer.valueDeserializer)
        put("group.id", config.consumer.groupId)
        put("auto.offset.reset", config.consumer.autoOffsetReset)
    }
    return KafkaConsumer<String, String>(consumerConfig)
}