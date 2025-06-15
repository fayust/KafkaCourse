package ru.job4j.kafka.configuration

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.bind.ConstructorBinding


@ConfigurationProperties(prefix = "spring.kafka")
data class KafkaProperties @ConstructorBinding constructor(
    val bootstrapServers: String,
    val consumer: ConsumerProperties,
    val producer: ProducerProperties,
    val taskTopic: String,
    val messageTopic: String,
    val messageEventTopic: String
    ) {
        data class ConsumerProperties @ConstructorBinding constructor(
            var groupId: String,
            var autoOffsetReset: String,
            var keyDeserializer: String,
            var valueDeserializer: String
        )

        data class ProducerProperties @ConstructorBinding constructor(
            var keySerializer: String,
            var valueSerializer: String
        )
}