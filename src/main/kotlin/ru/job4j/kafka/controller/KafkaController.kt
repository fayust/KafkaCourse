package ru.job4j.kafka.controller

import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import ru.job4j.kafka.model.RunParams
import ru.job4j.kafka.service.consumer.KafkaConsumerService
import ru.job4j.kafka.service.producer.KafkaProducerService
import ru.job4j.kafka.service.reqreply.ReqReplyService
import ru.job4j.kafka.service.reqreply.ReqReplyNoKafkaService


@RestController
class KafkaController (private val consumerService: KafkaConsumerService,
                       private val producerService: KafkaProducerService,
                       private val reqReplyNoKafkaService: ReqReplyNoKafkaService,
                       private val reqReplyService: ReqReplyService) {


    @PostMapping(value = ["/kafka/producer_run"])
    fun runKafkaProducer(): ResponseEntity<Void> {
        producerService.startProducer()
        return ResponseEntity.ok().build()
    }

    @PostMapping(value = ["/kafka/consumer_run"], consumes = [MediaType.APPLICATION_JSON_VALUE])
    fun runKafkaConsumer(@RequestBody params: RunParams): ResponseEntity<Void> {
        val consumerNum = params.consumerStartQuantity
        if (consumerNum > 0)
            consumerService.startMainConsumer()
        if (consumerNum > 1)
            consumerService.startSecondConsumer()
        if (consumerNum > 2)
            consumerService.startThirdConsumer()
        return ResponseEntity.ok().build()
    }

    @PostMapping(value = ["/req_reply_run"])
    fun reqReplyTemplate(@RequestBody params: RunParams): ResponseEntity<Void> {
        reqReplyNoKafkaService.processReqReply(params.reqReplyStartQuantity)
        return ResponseEntity.ok().build()
    }

    @PostMapping(value = ["/kafka/req_reply_run"])
    fun reqReplyKafkaCall(@RequestBody params: RunParams): ResponseEntity<String> {
        return ResponseEntity.ok().body(reqReplyService.startMessageEvent(params.msg))
    }


}