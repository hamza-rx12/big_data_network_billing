package me.hamza.config;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConfig {
    public static final String BOOTSTRAP_SERVERS = "kafka:9092";
    public static final String GROUP_ID_CONFIG = "flink consumer";

    public static KafkaSource<String> createKafkaSource(String topic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(topic)
                .setGroupId(GROUP_ID_CONFIG)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("metrics.context", "kafka.consumer")
                .build();
    }
}