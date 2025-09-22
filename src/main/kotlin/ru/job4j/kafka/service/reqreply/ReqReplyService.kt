package ru.job4j.kafka.service.reqreply

import org.springframework.stereotype.Service
import ru.job4j.kafka.service.consumer.KafkaConsumerService
import ru.job4j.kafka.service.producer.KafkaProducerService
import java.util.concurrent.CountDownLatch

@Service
class ReqReplyService(private val consumerService: KafkaConsumerService,
                      private val producerService: KafkaProducerService) {
    private val latch = CountDownLatch(1)

    /**
     *  Реализация шаблона ReqReply с Кафкой.
     *  Продюсер отправляет запрос в Кафку, блокируется в ожидании ответа и, получив его из того же топика,
     *  сразу отдает ответ клиенту
     */
    fun startMessageEvent(msg: String) : String  {
        producerService.sendSingleToMessageEvent(msg)
        var record = consumerService.startMessageEventConsumerBlocking(latch)
        latch.await()
        return record?.value() ?: "" // Возвращаем значение или пустую строку, если record равен null
    }
}

