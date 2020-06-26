package dev.thackba.kafkaconsumer

import dev.thackba.transfer.envelope.v1.EnvelopeProto
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

import dev.thackba.transfer.person.v1.PersonProto as V1Person
import dev.thackba.transfer.person.v2.PersonProto as V2Person

@Component
class Receiver {

    @KafkaListener(topics = ["test"], groupId = "consumer")
    fun listen(message: ByteArray) {
        val envelope = EnvelopeProto.Envelope.parseFrom(message)
        when (envelope.header.content) {
            "person" -> when (envelope.header.version) {
                1 -> {
                    val person = V1Person.Person.parseFrom(envelope.payload)
                    println("Received Person (Version 1): $person")
                }
                2 -> {
                    val person = V2Person.Person.parseFrom(envelope.payload)
                    println("Received Person (Version 2): $person")
                }
                else -> {
                    println("Unknown Person Version ${envelope.header.version}")
                }
            }
            else -> {
                println("Unknown Content ${envelope.header}")
            }
        }
    }
}