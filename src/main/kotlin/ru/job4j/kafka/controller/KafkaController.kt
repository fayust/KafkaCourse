package ru.job4j.kafka.controller

import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import ru.job4j.kafka.model.KafkaRunParams
import ru.job4j.kafka.service.consumer.KafkaConsumerService
import ru.job4j.kafka.service.producer.KafkaProducerService
import ru.job4j.kafka.service.reqreply.ReqReplyService


@RestController
@RequestMapping("/kafka")
class KafkaController (private val kafkaConsumerService: KafkaConsumerService,
                       private val kafkaProducerService: KafkaProducerService,
                       private val reqReplyService: ReqReplyService) {


    @PostMapping(value = ["/producer_run"])
    fun runKafkaProducer(): ResponseEntity<Void> {
        kafkaProducerService.startProducer()
        return ResponseEntity.ok().build()
    }

    @PostMapping(value = ["/consumer_run"], consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun runKafkaConsumer(@RequestBody params: KafkaRunParams): ResponseEntity<Void> {
        val consumerNum = params.consumerStartQuantity
        if (consumerNum > 0)
            kafkaConsumerService.startMainConsumer()
        if (consumerNum > 1)
            kafkaConsumerService.startSecondConsumer()
        if (consumerNum > 2)
            kafkaConsumerService.startThirdConsumer()
        return ResponseEntity.ok().build()
    }

    @PostMapping(value = ["/req_reply_run"])
    fun runReqReplyTemplate(@RequestBody params: KafkaRunParams): ResponseEntity<Void> {
        reqReplyService.processReqReply(params.reqReplyStartQuantity)
        return ResponseEntity.ok().build()
    }


}