package me.hamza.generators;

import me.hamza.DTOs.VoiceCallRecord;

import java.time.Instant;
import java.util.Random;
import com.github.javafaker.Faker;

public class VoiceCallRecordGenerator {

    private static final Faker faker = new Faker();
    private static final Random random = new Random();

    public static VoiceCallRecord generateRecord() {
        return new VoiceCallRecord(
            Instant.now().minusSeconds(random.nextInt(86400)).toString(),
            "+212 " + faker.options().option("6", "7") + faker.number().digits(8),
            "+212 " + faker.options().option("6", "7") + faker.number().digits(8),
            random.nextInt(1024),
            faker.address().cityName().toUpperCase() + "_" + random.nextInt(50),
            faker.options().option("2G", "3G", "4G")
        );
    }
}