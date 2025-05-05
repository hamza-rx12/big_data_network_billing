package me.hamza;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class MyConsumer {

    public static void main(String[] args) {
        // Set up the consumer properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group-id");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Consumer<String, String> consumer = null;

        try {
            // Create the consumer
            consumer = new KafkaConsumer<>(props);

            // Subscribe to the topic
            consumer.subscribe(Collections.singletonList("voice-calls"));

            // Continuously poll for new messages
            while (true) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(),
                                record.value());
                    }
                } catch (Exception e) {
                    System.err.println("Error during poll: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.err.println("Error creating or using consumer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Close the consumer
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (Exception e) {
                    System.err.println("Error closing consumer: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }
    }
}