package ru.job4j.kafka.service.consumer

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.springframework.stereotype.Service
import ru.job4j.kafka.configuration.KafkaProperties
import java.time.Duration

@Service
class KafkaConsumerService(private val kafkaConsumer: KafkaConsumer<String, String>,
                           private val kafkaProperties: KafkaProperties) {

    fun startMainConsumer() {
        val topic = kafkaProperties.messageTopic
        val firstConsumerThread = Thread { // Запускаем первого потребителя
            kafkaConsumer.use { consumer ->
                consumer.subscribe(listOf(topic))
                try {
                    while (true) {
                        consumer.poll(Duration.ofMillis(1000)).forEach { rec ->
                            println("First consumer received message: key=${rec.key()}, value=${rec.value()}, partition=${rec.partition()}, offset=${rec.offset()}")
                        }
                    }
                } catch (e: Exception) {
                    println("First consumer thread interrupted: ${e.message}")
                }
            }
        }
        firstConsumerThread.start()
        Runtime.getRuntime().addShutdownHook(Thread {
            println("Stopping First consumer ...")
            firstConsumerThread.interrupt() // Останавливаем первый поток при остановке приложения.
        })
    }


    /**
     * Cтарт второго потребителя
     */
    fun startSecondConsumer() {
        val topic = kafkaProperties.messageTopic
        val secondConsumerThread = Thread {
            kafkaConsumer.use { consumer ->
                consumer.subscribe(listOf(topic))
                try {
                    while (true) {
                        val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofMillis(1000))
                        records.forEach { rec ->
                            println("Second consumer received message: key=${rec.key()}, value=${rec.value()}, partition=${rec.partition()}, offset=${rec.offset()}")
                        }
                    }
                } catch (e: Exception) {
                    println("Second consumer thread interrupted: ${e.message}")
                }
            }
        }
        secondConsumerThread.start()
        Runtime.getRuntime().addShutdownHook(Thread {
            println("Stopping Second consumer...")
            secondConsumerThread.interrupt() // Останавливаем первый поток при остановке приложения.
        })
    }


    /**
     * Cтарт третьего потребителя
     */
    fun startThirdConsumer() {
        val topic = kafkaProperties.messageTopic
        val thirdConsumerThread = Thread {
            kafkaConsumer.use { consumer ->
                consumer.subscribe(listOf(topic))
                try {
                    while (true) {
                        val records: ConsumerRecords<String, String> = consumer.poll(Duration.ofMillis(1000))
                        records.forEach { rec ->
                            println("Third consumer received message: key=${rec.key()}, value=${rec.value()}, partition=${rec.partition()}, offset=${rec.offset()}")
                        }
                    }
                } catch (e: Exception) {
                    println("Third consumer thread interrupted: ${e.message}")
                }
            }
        }
        thirdConsumerThread.start()
        Runtime.getRuntime().addShutdownHook(Thread {
            println("Stopping Third consumer ...")
            thirdConsumerThread.interrupt() // Останавливаем третий поток при остановке приложения.
        })
    }
}