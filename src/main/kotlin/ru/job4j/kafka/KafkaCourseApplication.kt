package ru.job4j.kafka

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.runApplication
import ru.job4j.kafka.configuration.KafkaProperties

@SpringBootApplication
@EnableConfigurationProperties(KafkaProperties::class)
class KafkaCourseApplication

fun main(args: Array<String>) {
    runApplication<KafkaCourseApplication>(*args)
    println("start Kafka Spring microservice...")
}
