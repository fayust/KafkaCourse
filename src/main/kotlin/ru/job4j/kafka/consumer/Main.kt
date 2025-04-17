package ru.job4j.kafka.consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import java.time.Duration
import java.util.*

fun main() {
    val topic = "tasks"
    val server = "localhost:9092"
    newConsumer(server).use { consumer ->
        consumer.subscribe(listOf(topic))
        while (true) {
            consumer.poll(Duration.ofMillis(1000)).forEach { rec ->
                println("Received message: key=${rec.key()}, value=${rec.value()}, partition=${rec.partition()}, offset=${rec.offset()}")
           }
        }
    }
}

fun newConsumer(server: String): KafkaConsumer<String, String> {
    val config = Properties().apply {
        put("bootstrap.servers", server)
        put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        put("group.id", "my-consumer-group")
        put("auto.offset.reset", "earliest")
    }
    return KafkaConsumer<String, String>(config)
}