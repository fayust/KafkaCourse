package ru.job4j.kafka.consumer

import org.apache.kafka.clients.consumer.KafkaConsumer
import ru.job4j.kafka.config.KafkaConfig
import ru.job4j.kafka.config.loadConfig
import java.time.Duration
import java.util.*

fun main() {
    val config = loadConfig("src/main/resources/application.yaml")
    val topic = config.taskTopic
    newConsumer(config).use { consumer ->
        consumer.subscribe(listOf(topic))
        while (true) {
            consumer.poll(Duration.ofMillis(1000)).forEach { rec ->
                println("Received message: key=${rec.key()}, value=${rec.value()}, partition=${rec.partition()}, offset=${rec.offset()}")
           }
        }
    }
}


fun newConsumer(config: KafkaConfig): KafkaConsumer<String, String> {
    val consumerConfig = Properties().apply {
        put("bootstrap.servers", config.bootstrapServers)
        put("key.serializer", config.consumer.keyDeserializer)
        put("value.serializer", config.consumer.valueDeserializer)
        put("group.id", config.consumer.groupId)
        put("auto.offset.reset", config.consumer.autoOffsetReset)
    }
    return KafkaConsumer<String, String>(consumerConfig)
}