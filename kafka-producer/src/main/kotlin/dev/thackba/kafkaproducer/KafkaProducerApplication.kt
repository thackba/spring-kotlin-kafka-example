package dev.thackba.kafkaproducer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableScheduling
class KafkaProducerApplication

fun main(args: Array<String>) {
    runApplication<KafkaProducerApplication>(*args)
}
