package ru.job4j.kafka.domain.entity

import com.fasterxml.jackson.annotation.JsonProperty
import java.time.Instant

data class KafkaPayloadEvent (
    @JsonProperty("id") val id: String? = null,
    @JsonProperty("data") val data: String,
    @JsonProperty("createdAt") val createdAt: Instant
)
