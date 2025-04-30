package me.hamza.generators;

import java.util.function.Supplier;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;

public class GenericRecordGenerator<T> {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final Supplier<T> recordSupplier;

    public GenericRecordGenerator(Supplier<T> recordSupplier) {
        this.recordSupplier = recordSupplier;
    }

    public void generateStream() {
        Stream.generate(recordSupplier).forEach(record -> {
            try {
                String json = objectMapper.writeValueAsString(record);
                System.out.println(json);
                Thread.sleep(200);
            } catch (Exception e) {
                System.err.println("Couldn't map object to json!");
                e.printStackTrace();
            }

        });
    }
}