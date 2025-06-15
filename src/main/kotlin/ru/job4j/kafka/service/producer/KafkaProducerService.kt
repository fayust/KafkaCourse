package ru.job4j.kafka.service.producer

import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.springframework.stereotype.Service
import ru.job4j.kafka.configuration.KafkaProperties
import java.util.*

@Service
class KafkaProducerService(private val kafkaProducer: KafkaProducer<String, String>,
                           private val kafkaProperties: KafkaProperties) {


    fun startProducer() = runBlocking {
        val topic = kafkaProperties.messageTopic
        val rnd = Random().apply { setSeed(System.currentTimeMillis()) }

        launch { // Запускаем корутину
            kafkaProducer.use { producer ->
                val partitions = producer.partitionsFor(topic)
                partitions.forEach { partitionInfo ->
                    println("Partition: ${partitionInfo.partition()}, Leader: ${partitionInfo.leader()}")
                }
                repeat(Int.MAX_VALUE) { i ->
                    val msg = "Task $i"
                    val key = rnd.nextInt(3)
                    val meta = kafkaProducer.send(ProducerRecord(topic, key.toString(), msg))
                    println("text=$msg key=$key partition=${meta.get().partition()}")
                    delay(1000)
                }
            }
        }
    }

}



