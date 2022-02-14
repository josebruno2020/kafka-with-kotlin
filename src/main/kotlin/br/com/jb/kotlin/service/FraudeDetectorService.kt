package br.com.jb.kotlin.service

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Collections
import java.util.Properties

fun main() {
    val fraude = FraudeDetectorService()
}

class FraudeDetectorService() {
    private val consumer: KafkaConsumer<String, String>

    init {
        val properties: Properties = Properties()
        properties[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
        properties[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        properties[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java.name
        properties[ConsumerConfig.GROUP_ID_CONFIG] = FraudeDetectorService::class.simpleName
        properties[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 1
//        properties[ConsumerConfig.CLIENT_ID_CONFIG] = IPAddressName.NAME_IP
        this.consumer = KafkaConsumer(properties)
        consumer.subscribe(Collections.singletonList("PRIMEIRO_TOPICO"))

        consume()
    }

    fun consume() {
        while (true) {
            val records = consumer.poll(Duration.ofMillis(100))

            if (!records.isEmpty) {
                println("Encontrei ${records.count()} registros")
            }

            for (record in records) {
                println("------ Processsando -------- FRAUDE")
                println("${record.key()} / ${record.value()} - partition: ${record.partition()}")
                println("---------------")
            }
        }
    }
}