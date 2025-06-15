package ru.job4j.kafka.configuration

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.util.*

//Используется не спринговый KafkaTemplate, а стандартная библиотека Kafka
@Configuration
open class KafkaConfig(private val kafkaProperties: KafkaProperties) {

    @Bean
    open fun kafkaConsumer(): KafkaConsumer<String, String> {
        val consumerConfig = Properties().apply {
            put("bootstrap.servers", kafkaProperties.bootstrapServers)
            put("key.deserializer", kafkaProperties.consumer.keyDeserializer)
            put("value.deserializer", kafkaProperties.consumer.valueDeserializer)
            put("group.id", kafkaProperties.consumer.groupId)
            put("auto.offset.reset", kafkaProperties.consumer.autoOffsetReset)
        }
        return KafkaConsumer<String, String>(consumerConfig)
    }

    @Bean
    open fun kafkaProducer(): KafkaProducer<String, String> {
        val producerConfig = Properties().apply {
            put("bootstrap.servers", kafkaProperties.bootstrapServers)
            put("key.serializer", kafkaProperties.producer.keySerializer)
            put("value.serializer", kafkaProperties.producer.valueSerializer)
        }
        return KafkaProducer<String, String>(producerConfig)
    }
}
//fun loadConfig(): KafkaConfig  {
//    val filePath = "application.yaml"
//    val mapper = ObjectMapper(YAMLFactory())
//    val inputStream = KafkaConfig::class.java.classLoader.getResourceAsStream(filePath)
//        ?: throw IllegalArgumentException("File not found: $filePath")
//
//    return try {
//        mapper.readValue(inputStream, KafkaConfig::class.java)
//    } catch (e: Exception) {
//        e.printStackTrace()
//        throw RuntimeException("Failed to load configuration: ${e.message}", e)
//    }
//    val filePath = "src/main/resources/application.yaml"
//    val mapper = ObjectMapper(YAMLFactory())
//    return mapper.readValue(File(filePath), KafkaConfig::class.java)
