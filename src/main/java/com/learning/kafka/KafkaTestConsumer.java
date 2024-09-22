package com.learning.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaTestConsumer {

    public static void main(String[] args) {
        // Setup Properties for consumer
        Properties kafkaProps = new Properties();

        // List of Kafka brokers to connect to
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");

        // Deserializer class to convert Keys and Values from Byte Array to String
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        // Consumer Group ID for this consumer
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG,
                "kafka-java-consumer");

        // Set to consume from the earliest message when no offset is available
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");

        // Create a Consumer
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaProps)) {

             //Subscribe to the kafka.learning.orders topic
            consumer.subscribe(Arrays.asList("kafka.orders"));

            // Continuously poll for new messages
            while (true) {
                // Poll with a timeout of 100 milliseconds
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                // Print batch of records consumed
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Message fetched: Key = %s, Value = %s, Offset = %d%n",
                            record.key(), record.value(), record.offset());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
