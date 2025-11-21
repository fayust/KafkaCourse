package ru.job4j.kafka.service.outbox

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import ru.job4j.kafka.domain.entity.OutboxBusinessEntity
import ru.job4j.kafka.domain.entity.OutboxEvent
import ru.job4j.kafka.domain.entity.model.OutboxDto
import ru.job4j.kafka.repository.outbox.BusinessRepository
import ru.job4j.kafka.repository.outbox.OutboxRepository
import java.time.Instant

@Service
class OutboxBusinessService(private val objectMapper: ObjectMapper,
                            private val businessRepository: BusinessRepository,
                            private val outboxRepository: OutboxRepository) {

    @Transactional
    fun executeBusinessAndOutbox(outboxDto: OutboxDto) {
        val businessEntity = OutboxBusinessEntity(outboxDto)
        businessRepository.save(businessEntity)

        val outboxEvent = OutboxEvent(
            payload = objectMapper.writeValueAsString(businessEntity),
            createdAt = Instant.now(),
            processed = false
        )
        outboxRepository.save(outboxEvent)
    }
}
