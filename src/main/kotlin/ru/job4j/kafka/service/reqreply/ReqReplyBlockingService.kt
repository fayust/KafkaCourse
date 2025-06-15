package ru.job4j.kafka.service.reqreply

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.springframework.stereotype.Service
import ru.job4j.kafka.service.consumer.KafkaConsumerService
import ru.job4j.kafka.service.producer.KafkaProducerService
import java.util.concurrent.CountDownLatch

@Service
class ReqReplyBlockingService(private val kafkaConsumerService: KafkaConsumerService,
                                      private val kafkaProducerService: KafkaProducerService) {
    private val latch = CountDownLatch(1)

    fun startMessageEvent(msg: String) : String  {
        kafkaProducerService.sendSingleToMessageEvent(msg)
        var record : ConsumerRecord<String, String>? = null
        Thread {
            record = kafkaConsumerService.startMessageEventConsumerBlocking()
            latch.countDown()
        }.start()
        latch.await()
        return record?.value() ?: "" // Возвращаем значение или пустую строку, если record равен null
    }
}
