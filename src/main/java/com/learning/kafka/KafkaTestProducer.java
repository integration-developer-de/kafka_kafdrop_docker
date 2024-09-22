package com.learning.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaTestProducer {

    // Kafka Configuration Constants
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";
    private static final String TOPIC_NAME = "kafka.orders";
    private static final int MESSAGE_COUNT = 50;
    private static final long MESSAGE_INTERVAL_MS = 1000L;

    public static void main(String[] args) {

        // Setup Properties for Kafka Producer
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Use try-with-resources to ensure KafkaProducer is closed automatically
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps)) {

            // Generate a random start key
            int startKey = (new Random()).nextInt(1000);

            // Publish messages at regular intervals
            for (int i = 0; i < MESSAGE_COUNT; i++) {
                String key = String.valueOf(startKey + i);  // Key for the message
                String value = "This is order " + key;      // Message Content

                // Create a ProducerRecord
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, key, value);

                // Send message and get metadata for the sent record (sync send)
                try {
                    Future<RecordMetadata> future = producer.send(record);
                    RecordMetadata metadata = future.get();  // Synchronous to ensure it's sent
                    System.out.printf("Sent message with key=%s, value=%s to partition=%d, offset=%d%n",
                            key, value, metadata.partition(), metadata.offset());
                } catch (ExecutionException | InterruptedException e) {
                    System.err.println("Error sending message with key=" + key + ": " + e.getMessage());
                    // Restore interrupted state (important in case of InterruptedException)
                    Thread.currentThread().interrupt();
                }

                // Sleep between messages
                Thread.sleep(MESSAGE_INTERVAL_MS);
            }

        } catch (InterruptedException e) {
            // Handle interruption during sleep
            System.err.println("Producer thread interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // General exception handling for Kafka producer failures
            System.err.println("Unexpected error in Kafka producer: " + e.getMessage());
        }
    }
}
