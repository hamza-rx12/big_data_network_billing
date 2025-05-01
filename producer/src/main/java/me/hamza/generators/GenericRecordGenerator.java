package me.hamza.generators;

import java.util.function.Supplier;
import java.util.stream.Stream;

public class GenericRecordGenerator<T> {
    private final Supplier<T> supplier;

    public GenericRecordGenerator(Supplier<T> supplier) {
        this.supplier = supplier;
    }

    // Générer un flux d'enregistrements et imprimer les données
    public Stream<T> generateStream() {
        return Stream.generate(supplier)
                     .peek(record -> System.out.println("Generated Record: " + record));
    }
}