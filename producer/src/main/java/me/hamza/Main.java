package me.hamza;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.github.javafaker.Faker;

import me.hamza.DTOs.DataSessionRecord;
import me.hamza.DTOs.SMSRecord;
import me.hamza.DTOs.VoiceCallRecord;
import me.hamza.generators.GenericRecordGenerator;
// import me.hamza.generators.VoiceCallRecordGenerator;
// import java.util.concurrent.ExecutorService;
// import java.util.concurrent.Executors;

public class Main {
    // public static void main(String[] args) {
    // VoiceCallRecordGenerator foo = new VoiceCallRecordGenerator();
    // foo.generateStream();
    // }
    // private static final String TOPIC1 = "voice-calls";
    // private static final String TOPIC2 = "SMS-messages";
    // private static final String TOPIC3 = "Data-session-usage";
    // private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Faker faker = new Faker();
        Random random = new Random();
        // Properties props = new Properties();
        // props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        // props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        // StringSerializer.class.getName());
        // props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        // StringSerializer.class.getName());
        ExecutorService executor = Executors.newFixedThreadPool(3);

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

        executor.submit(dataSessionGenerator::generateStream);
        executor.submit(voiceCallGenerator::generateStream);
        executor.submit(smsRecordGenerator::generateStream);

        executor.shutdown();
    }

}
