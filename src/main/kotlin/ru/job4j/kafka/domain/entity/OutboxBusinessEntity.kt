package ru.job4j.kafka.domain.entity

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.Field
import ru.job4j.kafka.domain.entity.model.OutboxDto
import java.time.Instant

@Document(collection = "outbox_business")
data class OutboxBusinessEntity(
    @Id
    val id: String? = null,

    @Field("data")
    val data: String = "",

    @Field("created_at")
    val createdAt: Instant
) {
    constructor(outboxDto: OutboxDto) : this(
        id = null,
        data = outboxDto.data,
        createdAt = Instant.now()
    )
}