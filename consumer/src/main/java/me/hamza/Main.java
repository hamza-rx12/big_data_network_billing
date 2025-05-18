package me.hamza;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Main {

    public static final String BOOTSTRAP_SERVERS = "kafka:9092";
    public static final String GROUP_ID_CONFIG = "flink consumer";
    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) {
        env.enableCheckpointing(1000); // e.g., every 5 seconds
        KafkaSource<String> voiceCallSource = KafkaSource.<String>builder().setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics("voice-calls")
                .setGroupId(GROUP_ID_CONFIG)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .setProperty("commit.offsets.on.checkpoint", "true") // Explicit offset commit
                .setProperty("metrics.context", "kafka.consumer") // Enable metrics
                .build();

        KafkaSource<String> smsMessagesSource = KafkaSource.<String>builder().setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics("SMS-messages")
                .setGroupId(GROUP_ID_CONFIG)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();

        KafkaSource<String> dataUsageSource = KafkaSource.<String>builder().setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics("Data-session-usage")
                .setGroupId(GROUP_ID_CONFIG)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();

        DataStream<String> voiceCalls = env.fromSource(voiceCallSource, WatermarkStrategy.noWatermarks(),
                BOOTSTRAP_SERVERS);
        DataStream<String> smsMessages = env.fromSource(smsMessagesSource, WatermarkStrategy.noWatermarks(),
                BOOTSTRAP_SERVERS);
        DataStream<String> dataUsage = env.fromSource(dataUsageSource, WatermarkStrategy.noWatermarks(),
                BOOTSTRAP_SERVERS);

        ///////////////////////////////
        /// TESTING ///////////////////
        ///////////////////////////////
        DataStream<String> processedVoiceCalls = voiceCalls.map(record -> {
            // Example: Add a prefix to the record
            String processedRecord = "Processed Voice Call: " + record;
            System.out.println(processedRecord);
            return processedRecord;
        }).name("Processed Voice Calls");

        DataStream<String> processedSmsMessages = smsMessages.map(record -> {
            // Example: Convert the record to uppercase
            String processedRecord = record.toUpperCase();
            System.out.println("Processed SMS Message: " + processedRecord);
            return processedRecord;
        }).name("Processed SMS Messages");

        DataStream<String> processedDataUsage = dataUsage.map(record -> {
            // Example: Append a suffix to the record
            String processedRecord = record + " [Data Usage Processed]";
            System.out.println(processedRecord);
            return processedRecord;
        }).name("Processed Data Usage");

        // Example: Merge all processed streams into one
        DataStream<String> mergedStream = processedVoiceCalls.union(processedSmsMessages, processedDataUsage);

        // Print the merged stream
        mergedStream.map(record -> {
            System.out.println("Merged Record: " + record);
            return record;
        }).name("Merged Stream Processor");

        // voiceCalls.map(record -> {
        // System.out.println("Voice Call Record: " + record);
        // return record;
        // }).name("Voice Calls Processor");

        // smsMessages.map(record -> {
        // System.out.println("SMS Message Record: " + record);
        // return record;
        // }).name("SMS Messages Processor");

        // dataUsage.map(record -> {
        // System.out.println("Data Usage Record: " + record);
        // return record;
        // }).name("Data Usage Processor");

        // Execute the Flink job
        try {
            // The topics need to be already created to not have the consumer shut down
            waitForTopics(BOOTSTRAP_SERVERS, Arrays.asList("voice-calls", "SMS-messages", "Data-session-usage"));

            // Now we can consume without a problem after the topics are created
            env.execute("Kafka Multi-Topic Consumer");
        } catch (Exception e) {
            System.err.println("Problem in flink consumer!");
            e.printStackTrace();
        }

    }

    public static void waitForTopics(String bootstrapServers, List<String> topics) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        try (AdminClient admin = AdminClient.create(props)) {
            while (true) {
                ListTopicsResult topicsResult = admin.listTopics();
                Set<String> existingTopics = topicsResult.names().get();
                if (existingTopics.containsAll(topics)) {
                    break;
                }
                System.out.println("Waiting for topics to be created...");
                Thread.sleep(2000);
            }
        }
    }

}

// package me.hamza;

// import java.time.Duration;
// import java.util.Collections;
// import java.util.Properties;

// import org.apache.kafka.clients.consumer.Consumer;
// import org.apache.kafka.clients.consumer.ConsumerConfig;
// import org.apache.kafka.clients.consumer.ConsumerRecord;
// import org.apache.kafka.clients.consumer.ConsumerRecords;
// import org.apache.kafka.clients.consumer.KafkaConsumer;
// import org.apache.kafka.common.serialization.StringDeserializer;

// public class Main {

// private static final String TOPIC = "voice-calls";
// private static final String BOOTSTRAP_SERVERS = "kafka:9092"; // Match your
// Kafka broker address

// public static void main(String[] args) {
// // Consumer configuration
// Properties props = new Properties();
// props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
// props.put(ConsumerConfig.GROUP_ID_CONFIG, "voice-call-consumer-group"); //
// Unique group ID
// props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
// StringDeserializer.class.getName());
// props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
// StringDeserializer.class.getName());
// props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Read from
// the beginning

// // Create Kafka consumer
// try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
// consumer.subscribe(Collections.singletonList(TOPIC)); // Subscribe to the
// topic

// System.out.println("Listening for messages on topic: " + TOPIC);

// // Poll for new messages indefinitely
// while (true) {
// ConsumerRecords<String, String> records =
// consumer.poll(Duration.ofMillis(1000)); // Wait up to 1s
// for (ConsumerRecord<String, String> record : records) {
// System.out.println("Received record:");
// System.out.println(record.value()); // Print the JSON message
// System.out.println("---");
// }
// }
// }
// }
// }
