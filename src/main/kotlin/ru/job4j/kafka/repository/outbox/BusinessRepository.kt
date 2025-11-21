package ru.job4j.kafka.repository.outbox

import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.stereotype.Repository
import ru.job4j.kafka.domain.entity.OutboxBusinessEntity

@Repository
class BusinessRepository(private val mongoTemplate: MongoTemplate) {

    fun save(businessEntity: OutboxBusinessEntity) {
        mongoTemplate.save(businessEntity)
    }
}