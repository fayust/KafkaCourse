package ru.job4j.kafka.service.outbox

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.annotation.PreDestroy
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.springframework.boot.CommandLineRunner
import org.springframework.stereotype.Service
import ru.job4j.kafka.configuration.KafkaProperties
import ru.job4j.kafka.domain.entity.KafkaPayloadEvent
import java.util.concurrent.atomic.AtomicBoolean
import java.time.Duration

@Service
class OutboxConsumerService (private val objectMapper: ObjectMapper,
                             private val kafkaConsumer: KafkaConsumer<String, String>,
                             private val kafkaProperties: KafkaProperties)  : CommandLineRunner {

    private val running = AtomicBoolean(true)
    private val topic = kafkaProperties.messageEventTopic

    override fun run(vararg args: String?) {
        kafkaConsumer.subscribe(listOf(topic))

        while (running.get()) {
            val records = kafkaConsumer.poll(Duration.ofMillis(1000))
            for (record in records) {
                try {
                    println("Получено: key=${record.key()}, offset=${record.offset()}")
                    val event = objectMapper.readValue(record.value(), KafkaPayloadEvent::class.java)
                    println("Обработка записи ${event}")
                    kafkaConsumer.commitSync()
                } catch (e: Exception) {
                    println("Ошибка обработки записи ${record.offset()}")
                    println(e.toString())
                }
            }
        }
    }

    @PreDestroy
    fun shutdown() {
        running.set(false)
        kafkaConsumer.close()
    }

}