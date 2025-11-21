package ru.job4j.kafka.domain.entity

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.data.mongodb.core.mapping.Field
import java.time.Instant

@Document(collection = "outbox")
data class OutboxEvent(
    @Id
    val id: String? = null,

    @Field("payload")
    val payload: String,

    @Field("created_at")
    val createdAt: Instant = Instant.now(),

    @Field("processed")
    var processed: Boolean = false,

    @Field("processed_at")
    var processedAt: Instant? = null
)