package ru.job4j.kafka

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import ru.job4j.kafka.configuration.KafkaProperties

@SpringBootApplication
@EnableConfigurationProperties(KafkaProperties::class)
open class KafkaCourseApplication

fun main(args: Array<String>) {
    runApplication<KafkaCourseApplication>(*args)
    print("start Kafka Spring microservice...")
}
