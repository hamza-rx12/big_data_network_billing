package me.hamza;

import me.hamza.DTOs.DataSessionRecord;
import me.hamza.DTOs.SMSRecord;
import me.hamza.DTOs.VoiceCallRecord;
import me.hamza.generators.GenericRecordGenerator;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;

public class Main {
    private static final String BOOTSTRAP_SERVERS = "kafka:9092"; // Kafka broker address
    private static final String TOPIC_DATA_SESSION = "data-session";
    private static final String TOPIC_VOICE_CALL = "voice-call";
    private static final String TOPIC_SMS = "sms";

    public static void main(String[] args) {
        Faker faker = new Faker();
        Random random = new Random();
        ExecutorService executor = Executors.newFixedThreadPool(3); // Pool de 3 threads

        // Kafka producer configuration
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            // Générateur pour DataSessionRecord
            GenericRecordGenerator<DataSessionRecord> dataSessionGenerator = new GenericRecordGenerator<>(
                () -> new DataSessionRecord(
                    Instant.now().minusSeconds(random.nextInt(86400)).toString(),
                    "+212 " + faker.number().digits(9),
                    random.nextDouble() * 1000,
                    random.nextInt(3600),
                    faker.address().cityName().toUpperCase() + "_" + random.nextInt(50),
                    faker.options().option("LTE", "5G")
                )
            );

            // Générateur pour VoiceCallRecord
            GenericRecordGenerator<VoiceCallRecord> voiceCallGenerator = new GenericRecordGenerator<>(
                () -> new VoiceCallRecord(
                    Instant.now().minusSeconds(random.nextInt(86400)).toString(),
                    "+212 " + faker.options().option("6", "7") + faker.number().digits(8),
                    "+212 " + faker.options().option("6", "7") + faker.number().digits(8),
                    random.nextInt(1024),
                    faker.address().cityName().toUpperCase() + "_" + random.nextInt(50),
                    faker.options().option("2G", "3G", "4G")
                )
            );

            // Générateur pour SMSRecord
            GenericRecordGenerator<SMSRecord> smsGenerator = new GenericRecordGenerator<>(
                () -> new SMSRecord(
                    Instant.now().minusSeconds(random.nextInt(86400)).toString(),
                    "+212 " + faker.options().option("6", "7") + faker.number().digits(8),
                    "+212 " + faker.options().option("6", "7") + faker.number().digits(8),
                    faker.address().cityName().toUpperCase() + "_" + random.nextInt(50),
                    faker.options().option("2G", "3G", "4G")
                )
            );

            // Soumettre les tâches au pool de threads
            executor.submit(() -> sendToKafka(dataSessionGenerator, producer, TOPIC_DATA_SESSION));
            executor.submit(() -> sendToKafka(voiceCallGenerator, producer, TOPIC_VOICE_CALL));
            executor.submit(() -> sendToKafka(smsGenerator, producer, TOPIC_SMS));

            // Arrêter le pool de threads après l'exécution des tâches
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
    }

    private static <T> void sendToKafka(GenericRecordGenerator<T> generator, Producer<String, String> producer, String topic) {
        ObjectMapper objectMapper = new ObjectMapper();
        generator.generateStream()
            //.limit(10) // Limiter à 10 enregistrements
            .forEach(record -> {
                try {
                    String json = objectMapper.writeValueAsString(record);
                    ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>(topic, json);

                    // Envoyer l'enregistrement à Kafka
                    producer.send(kafkaRecord, (metadata, exception) -> {
                        if (exception == null) {
                            System.out.printf(
                                "Sent record to topic %s, partition %d, offset %d%n",
                                metadata.topic(),
                                metadata.partition(),
                                metadata.offset()
                            );
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