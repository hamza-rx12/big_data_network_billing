package me.hamza;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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

import com.fasterxml.jackson.core.type.TypeReference;
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

    private static Faker faker = new Faker();
    private static Random random = new Random();
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static List<Long> ids = fetchIds();
    private static int lenIds = ids.size();

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        // props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 104857600);

        ExecutorService executor = Executors.newFixedThreadPool(3);

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            Float impurity = 0.15f;
            GenericRecordGenerator<DataSessionRecord> dataSessionGenerator = new GenericRecordGenerator<>(
                    () -> myDataSessionRecord(impurity));

            GenericRecordGenerator<VoiceCallRecord> voiceCallGenerator = new GenericRecordGenerator<>(
                    () -> myVoiceCallRecord(impurity));

            GenericRecordGenerator<SMSRecord> smsRecordGenerator = new GenericRecordGenerator<>(
                    () -> mySmsRecord(impurity));

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

    public static SMSRecord mySmsRecord(Float impurity) {
        return new SMSRecord(
                maybeNull(Instant.now().minusSeconds(random.nextInt(86400)).toString(),
                        impurity),
                maybeNull(String.valueOf(ids.get(random.nextInt(lenIds))), impurity),
                maybeNull(String.valueOf(ids.get(random.nextInt(lenIds))), impurity),
                maybeNull(faker.address().cityName().toUpperCase() + "_" +
                        random.nextInt(50), impurity),
                maybeNull(faker.options().option("LTE", "5G"), impurity));
    }

    public static VoiceCallRecord myVoiceCallRecord(Float impurity) {
        return new VoiceCallRecord(
                maybeNull(Instant.now().minusSeconds(random.nextInt(86400)).toString(),
                        impurity),
                maybeNull(String.valueOf(ids.get(random.nextInt(lenIds))), impurity),
                maybeNull(String.valueOf(ids.get(random.nextInt(lenIds))), impurity),
                // maybeNull(
                random.nextInt(1024),
                // impurity),
                maybeNull(faker.address().cityName().toUpperCase() + "_" +
                        random.nextInt(50), impurity),
                maybeNull(faker.options().option("2G", "3G", "4G"), impurity));
    }

    public static DataSessionRecord myDataSessionRecord(Float impurity) {
        return new DataSessionRecord(
                maybeNull(Instant.now().minusSeconds(random.nextInt(86400)).toString(),
                        impurity),
                maybeNull(String.valueOf(ids.get(random.nextInt(lenIds))), impurity),
                // maybeNull(
                random.nextDouble() * 1000,
                // impurity),
                // maybeNull(
                random.nextInt(3600),
                // impurity),
                maybeNull(faker.address().cityName().toUpperCase() + "_" +
                        random.nextInt(50), impurity),
                maybeNull(faker.options().option("LTE", "5G"), impurity));
    }

    public static <T> T maybeNull(T value, Float impurity) {
        return random.nextFloat() < impurity ? null : value;
    }

    private static <T> void processStream(Stream<T> stream, Producer<String, String> producer, String topic,
            ObjectMapper objectMapper) {
        stream.forEach(record -> {
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

    public static List<Long> fetchIds() {
        String apiUrl = "http://database:8080/api/customers/ids";

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(apiUrl))
                .GET()
                .build();

        try {
            // Thread.sleep(15000);
            HttpResponse<String> response = client.send(
                    request,
                    HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                // Parse the JSON response into a List<Long>
                ObjectMapper mapper = new ObjectMapper();
                return mapper.readValue(
                        response.body(),
                        new TypeReference<List<Long>>() {
                        });
            } else {
                System.err.println("API request failed with status code: " + response.statusCode());
                return new ArrayList<>();
            }
        } catch (Exception e) {
            System.err.println("Error fetching data from API: " + e.getMessage());
            return new ArrayList<>();
        }
    }

}
