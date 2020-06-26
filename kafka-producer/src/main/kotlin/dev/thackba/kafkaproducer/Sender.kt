package dev.thackba.kafkaproducer

import com.google.protobuf.ByteString
import dev.thackba.transfer.envelope.v1.EnvelopeProto
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.lang.IllegalStateException
import java.util.*

import dev.thackba.transfer.person.v1.PersonProto as V1Person
import dev.thackba.transfer.person.v2.PersonProto as V2Person

@Component
class Sender(val kafkaTemplate: KafkaTemplate<String, ByteArray>?) {

    private val logger = LoggerFactory.getLogger(Sender::class.java)

    @Scheduled(fixedRate = 5000)
    fun sender() {
        val id = UUID.randomUUID().toString()
        val version = Random().nextInt(2) + 1
        val payload: ByteArray = when (version) {
            1 -> V1Person.Person.newBuilder()
                    .setId(id)
                    .setName("Max Mustermann")
                    .setEmail("max@mustermann.de")
                    .build()
                    .toByteArray()
            2 -> V2Person.Person.newBuilder()
                    .setId(id)
                    .setFirstname("Max")
                    .setSurname("Mustermann")
                    .setEmail("max@mustermann.de")
                    .build()
                    .toByteArray()
            else -> throw IllegalStateException("Wrong Version found")
        }
        val message = EnvelopeProto.Envelope.newBuilder()
                .setHeader(EnvelopeProto.Header.newBuilder()
                        .setContent("person")
                        .setVersion(version)
                        .build())
                .setPayload(ByteString.copyFrom(payload))
                .build()
                .toByteArray()
        logger.info("Sending data in version " + version)
        kafkaTemplate?.send("test", message)
    }

}