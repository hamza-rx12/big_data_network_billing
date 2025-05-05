package me.hamza;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;

import me.hamza.DTOs.DataSessionRecord;
import me.hamza.DTOs.SMSRecord;
import me.hamza.DTOs.VoiceCallRecord;
import me.hamza.generators.GenericRecordGenerator;

public class Main {
    private static final String TOPIC1 = "voice-calls";
    private static final String TOPIC2 = "SMS-messages";
    private static final String TOPIC3 = "Data-session-usage";
    private static final String BOOTSTRAP_SERVERS = "kafka:9092";

    public static void main(String[] args) {
        Faker faker = new Faker();
        Random random = new Random();
        ObjectMapper objectMapper = new ObjectMapper();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 104857600);

        ExecutorService executor = Executors.newFixedThreadPool(3);

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            GenericRecordGenerator<DataSessionRecord> dataSessionGenerator = new GenericRecordGenerator<>(
                    () -> new DataSessionRecord(
                            Instant.now().minusSeconds(random.nextInt(86400)).toString(),
                            "+212 " + faker.number().digits(9),
                            random.nextDouble() * 1000,
                            random.nextInt(3600),
                            faker.address().cityName().toUpperCase() + "_" + random.nextInt(50),
                            faker.options().option("LTE", "5G")));

            GenericRecordGenerator<VoiceCallRecord> voiceCallGenerator = new GenericRecordGenerator<>(
                    () -> new VoiceCallRecord(
                            Instant.now().minusSeconds(random.nextInt(86400)).toString(),
                            "+212 " + faker.options().option("6", "7") + faker.number().digits(8),
                            "+212 " + faker.options().option("6", "7") + faker.number().digits(8),
                            random.nextInt(1024),
                            faker.address().cityName().toUpperCase() + "_" + random.nextInt(50),
                            faker.options().option("2G", "3G", "4G")));

            GenericRecordGenerator<SMSRecord> smsRecordGenerator = new GenericRecordGenerator<>(
                    () -> new SMSRecord(
                            Instant.now().minusSeconds(random.nextInt(86400)).toString(),
                            "+212 " + faker.options().option("6", "7") + faker.number().digits(8),
                            "+212 " + faker.options().option("6", "7") + faker.number().digits(8),
                            faker.address().cityName().toUpperCase() + "_" + random.nextInt(50),
                            faker.options().option("LTE", "5G")));

            executor.submit(() -> processStream(dataSessionGenerator.generateStream(), producer, TOPIC3, objectMapper));
            executor.submit(() -> processStream(voiceCallGenerator.generateStream(), producer, TOPIC1, objectMapper));
            executor.submit(() -> processStream(smsRecordGenerator.generateStream(), producer, TOPIC2, objectMapper));

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS); // Wait for all tasks to complete
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Executor interrupted: " + e.getMessage());
        }
    }

    private static <T> void processStream(Stream<T> stream, Producer<String, String> producer, String topic,
            ObjectMapper objectMapper) {
        stream.forEach(record -> { // Limit to 100 records per stream
            try {
                String json = objectMapper.writeValueAsString(record);
                ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(topic, json);

                producer.send(kafkaRecord, (metadata, exception) -> {
                    if (exception == null) {
                        // System.out.printf("Sent record to topic %s, partition %d, offset %d%n",
                        // topic, metadata.partition(), metadata.offset());
                    } else {
                        System.err.println("Failed to send record: " + exception.getMessage());
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }

}
