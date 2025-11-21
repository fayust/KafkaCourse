package ru.job4j.kafka.repository.outbox

import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.FindAndModifyOptions
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.stereotype.Repository
import ru.job4j.kafka.domain.entity.OutboxEvent
import java.time.Instant

@Repository
class OutboxRepository(private val mongoTemplate: MongoTemplate) {

    fun save(event: OutboxEvent) {
        mongoTemplate.save(event)
    }

    fun remove(event: OutboxEvent) {
        mongoTemplate.remove(event)
    }

    /**
     * Находим первое необработанное событие и помечаем как "в обработке"
     */
    fun findAndMarkAsProcessing(): OutboxEvent? {
        val query = Query().addCriteria(Criteria.where("processed").`is`(false))
                           .with(Sort.by("created_at").ascending())

        val update = Update().set("processed", true)
                             .set("processed_at", Instant.now())
        val options = FindAndModifyOptions().returnNew(true).upsert(false)
        return mongoTemplate.findAndModify(query, update, options, OutboxEvent::class.java)
    }


    fun unmarkAsProcessingFailed(event: OutboxEvent) {
        val query = Query().addCriteria(Criteria.where("id").`is`(event.id))
        val update = Update().set("processed", false)
                             .unset("processed_at")
        mongoTemplate.updateFirst(query, update, OutboxEvent::class.java)
    }
}