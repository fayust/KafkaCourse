package ru.job4j.kafka.model

data class KafkaRunParams (
    val consumerStartQuantity: Int,
    val reqReplyStartQuantity: Int,
    val msg: String,
)
