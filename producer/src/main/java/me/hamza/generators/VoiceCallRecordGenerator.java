package me.hamza.generators;

import java.time.Instant;
import java.util.Random;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;

import me.hamza.DTOs.VoiceCallRecord;

public class VoiceCallRecordGenerator {

    private static final Faker faker = new Faker();
    private static final Random random = new Random();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public void generateStream() {
        Stream.generate(VoiceCallRecordGenerator::generateRecord).forEach(
                record -> {
                    try {
                        String json = objectMapper.writeValueAsString(record);
                        System.out.println(json);

                    } catch (Exception e) {
                        System.err.println("Couldn't map object to json!");
                        e.printStackTrace();
                    }
                });
    }

    public static VoiceCallRecord generateRecord() {
        return new VoiceCallRecord(
                Instant.now().minusSeconds(random.nextInt(86400)).toString(),
                "+212 " + faker.number().digits(9),
                "+212 " + faker.number().digits(9),
                random.nextInt(1024),
                faker.address().cityName().toUpperCase() + "_" + random.nextInt(50),
                faker.options().option("2G", "3G", "4G"));
    }
}