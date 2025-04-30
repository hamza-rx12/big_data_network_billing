package me.hamza;

import java.util.Properties;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import me.hamza.generators.VoiceCallRecordGenerator;

public class MyProducer {

    private static final String TOPIC = "voice-calls";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092"; // Change if Kafka is remote

    public static void main(String[] args) {
        // Kafka producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Kafka producer
        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            Stream.generate(VoiceCallRecordGenerator::generateRecord)
                    // .limit(10) // Send 10 records (adjust as needed)
                    .forEach(record -> {
                        try {
                            String json = new ObjectMapper().writeValueAsString(record);
                            ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(TOPIC, json);

                            // Send record asynchronously
                            producer.send(kafkaRecord, (metadata, exception) -> {
                                if (exception == null) {
                                    System.out.printf(
                                            "Sent record to partition %d, offset %d%n",
                                            metadata.partition(),
                                            metadata.offset());
                                } else {
                                    System.err.println("Failed to send record: " + exception.getMessage());
                                }
                            });
                            // Thread.sleep(50);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
            producer.flush(); // Ensure all messages are sent
        }
    }

    // public static void main(String[] args) {
    // Properties props = new Properties();
    // props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    // props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
    // StringSerializer.class.getName());
    // props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
    // StringSerializer.class.getName());

    // Producer<String, String> producer = null;

    // try {
    // producer = new KafkaProducer<>(props);
    // ProducerRecord<String, String> record = new ProducerRecord<String,
    // String>("messages", "my_key",
    // "Hello kafka heey!");
    // producer.send(record, new Callback() {
    // @Override
    // public void onCompletion(RecordMetadata metadata, Exception exception) {
    // if (exception != null) {
    // System.err.println("Error sending record: " + exception.getMessage());
    // exception.printStackTrace();
    // } else {
    // System.out.println("Record sent successfully to topic " + metadata.topic() +
    // " partition " + metadata.partition() + " at offset " + metadata.offset());
    // }
    // }
    // });
    // } catch (Exception e) {
    // System.err.println("Error creating or sending record: " + e.getMessage());
    // e.printStackTrace();
    // } finally {
    // if (producer != null) {
    // try {
    // producer.close();
    // } catch (Exception e) {
    // System.err.println("Error closing producer: " + e.getMessage());
    // e.printStackTrace();
    // }
    // }
    // }

    // }
}
