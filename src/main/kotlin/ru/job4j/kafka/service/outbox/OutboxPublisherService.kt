package ru.job4j.kafka.service.outbox

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import ru.job4j.kafka.configuration.KafkaProperties
import ru.job4j.kafka.repository.outbox.OutboxRepository


@Service
class OutboxPublisherService(private val kafkaProperties: KafkaProperties,
                            private val outboxRepository: OutboxRepository,
                            private val kafkaTemplate: KafkaTemplate<String, String>) {

    private val batchSize = 10

    @Transactional
    @Scheduled(fixedDelay = 10000)
    fun publishOutbox() {
        val topic = kafkaProperties.messageEventTopic
        var processedCount = 0
        println("Публикация  OutboxEvent в Кафка")
        while (processedCount < batchSize) {
            val event = outboxRepository.findAndMarkAsProcessing() ?: break
            try {
                // Отправляем в Kafka и ждём подтверждения
                kafkaTemplate.send(topic, event.payload).get()
                outboxRepository.remove(event)
                processedCount++
            } catch (e: Exception) {
                println("Не получили ACK из кафка")
                outboxRepository.unmarkAsProcessingFailed(event)
                throw e
            }
        }
    }


}