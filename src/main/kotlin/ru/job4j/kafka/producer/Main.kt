package ru.job4j.kafka.producer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import java.time.Duration
import java.util.*
import kotlinx.coroutines.*
import org.apache.kafka.clients.producer.ProducerRecord

fun main() = runBlocking {
    val topic = "tasks"
    val server = "localhost:9092"
    newProducer(server).use { producer ->
        repeat(Int.MAX_VALUE) { i ->
            val msg = "Task $i"
            producer.send(ProducerRecord(topic, msg))
            println("Send $msg")
            delay(1000)
        }
    }
}

fun newProducer(server: String): KafkaProducer<String, String> {
    val config = Properties().apply {
        put("bootstrap.servers", server)
        put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    }
    return KafkaProducer<String, String>(config)
}

