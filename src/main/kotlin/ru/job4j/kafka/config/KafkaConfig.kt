package ru.job4j.kafka.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import java.io.File

data class KafkaConfig(
    val bootstrapServers: String,
    val producer: ProducerConfig,
    val consumer: ConsumerConfig,
    val taskTopic: String,
    val messageTopic: String
)

data class ProducerConfig(
    val keySerializer: String,
    val valueSerializer: String
)

data class ConsumerConfig(
    val groupId: String,
    val keyDeserializer: String,
    val valueDeserializer: String,
    val autoOffsetReset: String
)

fun loadConfig(filePath: String): KafkaConfig {
    val mapper = ObjectMapper(YAMLFactory())
    return mapper.readValue(File(filePath), KafkaConfig::class.java)
}