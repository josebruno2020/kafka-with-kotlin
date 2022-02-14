package br.com.jb.kotlin.service

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.*

fun main() {
    val email = EmailService()
}

class EmailService {
    private val consumer: KafkaConsumer<String, String>
    private val topicName: String = "TOPICO_SEND_EMAIL"

    init {

        val service = KafkaService(topicName, this.consume())
        service.run()

        val properties: Properties = Properties()
        properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        properties[ConsumerConfig.GROUP_ID_CONFIG] = EmailService::class.simpleName
        properties[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 1
        this.consumer = KafkaConsumer(properties)
        consumer.subscribe(Collections.singletonList("TOPICO_SEND_EMAIL"))

        consume()


    }

    fun consume() {
        while (true) {
            val records = this.consumer.poll(Duration.ofMillis(100))

            if (!records.isEmpty) {
                println("Encontrei ${records.count()} registros")
            }

            for (record in records) {
                println("------ Processsando --------")
                println("SEND EMAIL")
                println("${record.key()} / ${record.value()} - partition: ${record.partition()}")
                println("---------------")
            }
        }
    }
}