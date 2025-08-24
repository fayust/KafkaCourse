package ru.job4j.kafka.reqreply.integration

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import java.time.Duration
import java.util.Properties

class KafkaReqReplyTest {

    companion object {
        private lateinit var kafkaContainer: KafkaContainer
        private lateinit var producer: KafkaProducer<String, String>
        private lateinit var consumer: KafkaConsumer<String, String>
        private const val TOPIC = "test-topic"

        @JvmStatic
        @BeforeAll
        fun setup() {
            kafkaContainer = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:latest"))
            kafkaContainer.start()

            val producerProps = Properties().apply {
                put("bootstrap.servers", kafkaContainer.bootstrapServers)
                put("key.serializer", StringSerializer::class.java.name)
                put("value.serializer", StringSerializer::class.java.name)
            }
            producer = KafkaProducer(producerProps)

            val consumerProps = Properties().apply {
                put("bootstrap.servers", kafkaContainer.bootstrapServers)
                put("key.deserializer", StringDeserializer::class.java.name)
                put("value.deserializer", StringDeserializer::class.java.name)
                put("group.id", "test-group")
                put("auto.offset.reset", "earliest")
            }
            consumer = KafkaConsumer(consumerProps)
            consumer.subscribe(listOf(TOPIC))
        }

        @JvmStatic
        @AfterAll
        fun teardown() {
            producer.close()
            consumer.close()
            kafkaContainer.stop()
        }
    }

    @Test
    fun `test sending and receiving message`() {
        val message = "Hello, Kafka!"
        producer.send(ProducerRecord(TOPIC, message)).get()

        val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofSeconds(5))
        val receivedMessage = records.iterator().next().value()

        assertEquals(message, receivedMessage)
    }
}