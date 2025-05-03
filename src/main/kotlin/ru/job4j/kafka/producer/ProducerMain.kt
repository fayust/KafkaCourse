package ru.job4j.kafka.producer

import org.apache.kafka.clients.producer.KafkaProducer
import java.util.*
import kotlinx.coroutines.*
import org.apache.kafka.clients.producer.ProducerRecord
import ru.job4j.kafka.config.KafkaConfig
import ru.job4j.kafka.config.loadConfig

fun main() = runBlocking {
    val config = loadConfig()
    val topic = config.taskTopic // Используем топик из конфигурации
    newProducer(config).use { producer ->
        repeat(Int.MAX_VALUE) { i ->
            val msg = "Task $i"
            producer.send(ProducerRecord(topic, msg))
            println("Send $msg")
            delay(1000)
        }
    }
}

fun newProducer(config: KafkaConfig): KafkaProducer<String, String> {
    val producerConfig  = Properties().apply {
        put("bootstrap.servers", config.bootstrapServers)
        put("key.serializer", config.producer.keySerializer)
        put("value.serializer", config.producer.valueSerializer)
    }
    return KafkaProducer<String, String>(producerConfig)
}

