package br.com.jb.kotlin

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import java.lang.Exception
import java.util.*

class NewOrder {
    private val producer: KafkaProducer<String, String>

    init {
        println("----------------------- INICIALIZANDO KAFKA --------------------")

        val properties = Properties()
        properties[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
        properties[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        properties[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name

        this.producer = KafkaProducer(properties)
    }

    fun send(value: String): Unit {
        val key = UUID.randomUUID().toString()
        val record = ProducerRecord<String, String>("PRIMEIRO_TOPICO", key, value)
        val callbackFunction: (RecordMetadata, Exception?) -> Unit = { data, ex ->
            ex?.printStackTrace()
            println("Sucesso no envio: TOPICO -> ${data.topic()} ::: partition -> ${data.partition()} / timestamps ${data.timestamp()}")
        }
        producer.send(record, callbackFunction).get()

        val email = "Welcome! We are processing your order."
        val emailRecord = ProducerRecord<String, String>("TOPICO_SEND_EMAIL", email, email)
        producer.send(emailRecord, callbackFunction).get()

    }
}