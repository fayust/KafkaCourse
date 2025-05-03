package ru.job4j.kafka.config

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

data class KafkaConfig @JsonCreator constructor(
    @JsonProperty("bootstrap-servers") val bootstrapServers: String,
    @JsonProperty("producer") val producer: ProducerConfig,
    @JsonProperty("consumer") val consumer: ConsumerConfig,
    @JsonProperty("task-topic") val taskTopic: String,
    @JsonProperty("message-topic") val messageTopic: String
)

data class ProducerConfig(
    @JsonProperty("key-serializer") val keySerializer: String,
    @JsonProperty("value-serializer") val valueSerializer: String
)

data class ConsumerConfig  @JsonCreator constructor(
    @JsonProperty("group-id") val groupId: String,
    @JsonProperty("key-deserializer") val keyDeserializer: String,
    @JsonProperty("value-deserializer") val valueDeserializer: String,
    @JsonProperty("auto-offset-reset") val autoOffsetReset: String
)



fun loadConfig(): KafkaConfig  {
    val filePath = "application.yaml"
    val mapper = ObjectMapper(YAMLFactory())
    val inputStream = KafkaConfig::class.java.classLoader.getResourceAsStream(filePath)
        ?: throw IllegalArgumentException("File not found: $filePath")

    return try {
        mapper.readValue(inputStream, KafkaConfig::class.java)
    } catch (e: Exception) {
        e.printStackTrace()
        throw RuntimeException("Failed to load configuration: ${e.message}", e)
    }
//    val filePath = "src/main/resources/application.yaml"
//    val mapper = ObjectMapper(YAMLFactory())
//    return mapper.readValue(File(filePath), KafkaConfig::class.java)
}