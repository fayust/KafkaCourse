package ru.job4j.kafka.producer

import org.apache.kafka.clients.producer.KafkaProducer
import java.util.*
import kotlinx.coroutines.*
import org.apache.kafka.clients.producer.ProducerRecord
import ru.job4j.kafka.config.KafkaConfig
import ru.job4j.kafka.config.loadConfig

fun main(): Unit = runBlocking {
    val config = loadConfig()
    val topic = config.messageTopic
    val rnd = Random().apply { setSeed(System.currentTimeMillis()) }
    newProducer(config).use { producer ->
        val partitions = producer.partitionsFor(topic)
        partitions.forEach { partitionInfo ->
            println("Partition: ${partitionInfo.partition()}, Leader: ${partitionInfo.leader()}")
        }
        repeat(Int.MAX_VALUE) { i ->
            val msg = "Task $i"
            val key = rnd.nextInt(3)
            val meta = producer.send(ProducerRecord(topic, key.toString(), msg))
            println("text=$msg key=$key partition=${meta.get().partition()}")
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

