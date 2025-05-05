package me.hamza;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaMultiTopicConsumer {

    private static final String[] TOPICS = {"voice-calls", "data-session", "sms"};
    private static final String BOOTSTRAP_SERVERS = "kafka:9092"; // Match your Kafka broker address

    public static void main(String[] args) {
        // Consumer configuration
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "multi-topic-consumer-group"); // Unique group ID
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Read from the beginning

        // Create Kafka consumer
        try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(TOPICS)); // Subscribe to multiple topics

            System.out.println("Listening for messages on topics: " + String.join(", ", TOPICS));

            // Poll for new messages indefinitely
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); // Wait up to 1s
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received record from topic %s:%n", record.topic());
                    System.out.println(record.value()); // Print the JSON message
                    System.out.println("---");
                }
            }
        }
    }
}