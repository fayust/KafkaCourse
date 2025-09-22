package ru.job4j.kafka.service.producer

import kotlinx.coroutines.*
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.springframework.stereotype.Service
import ru.job4j.kafka.configuration.KafkaProperties
import java.util.*

@Service
class KafkaProducerService(private val kafkaProducer: KafkaProducer<String, String>,
                           private val kafkaProperties: KafkaProperties,
                           private val coroutineScope: CoroutineScope) {


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

    /**
     * Отправляет 1 сообщение в Кафку в корутине
     */
    fun sendSingleToMessageEvent(msg: String) {
        val topic = kafkaProperties.messageEventTopic
        val props = getProducerProps()
        val produserNew = KafkaProducer<String, String>(props)
        coroutineScope.launch {
            try {
                val metadata: RecordMetadata = produserNew.send(ProducerRecord(topic, msg)).get()
                println("Sent message='$msg' to topic='${metadata.topic()}' partition=${metadata.partition()} offset=${metadata.offset()}")
            } catch (e: Exception) {
                println("Error sending message='$msg': ${e.message}")
            }
        }
    }

    fun getProducerProps(): Properties {
        return Properties().apply {
            put("bootstrap.servers", kafkaProperties.bootstrapServers)
            put("key.serializer", kafkaProperties.producer.keySerializer)
            put("value.serializer", kafkaProperties.producer.valueSerializer)
            put("enable.idempotence", "true")
            put("acks", "all")
        }
    }

}



