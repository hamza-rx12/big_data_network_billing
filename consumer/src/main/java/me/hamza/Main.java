package me.hamza;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;

import me.hamza.config.KafkaConfig;
import me.hamza.processor.StreamProcessor;
import me.hamza.processor.StreamProcessor.StreamPair;

public class Main {
    public static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) {
        env.enableCheckpointing(1000);

        // Create Kafka sources
        var voiceCallSource = KafkaConfig.createKafkaSource("voice-calls");
        var smsMessagesSource = KafkaConfig.createKafkaSource("SMS-messages");
        var dataUsageSource = KafkaConfig.createKafkaSource("Data-session-usage");

        // Create data streams
        DataStream<String> voiceCalls = env.fromSource(voiceCallSource,
                WatermarkStrategy.noWatermarks(), KafkaConfig.BOOTSTRAP_SERVERS);
        DataStream<String> smsMessages = env.fromSource(smsMessagesSource,
                WatermarkStrategy.noWatermarks(), KafkaConfig.BOOTSTRAP_SERVERS);
        DataStream<String> dataUsage = env.fromSource(dataUsageSource,
                WatermarkStrategy.noWatermarks(), KafkaConfig.BOOTSTRAP_SERVERS);

        // Process streams
        StreamProcessor processor = new StreamProcessor("kafka:9092");

        // Process individual streams and get their valid/invalid outputs
        StreamPair validVoiceCalls = processor.processVoiceCalls(voiceCalls);
        StreamPair validSmsMessages = processor.processSmsMessages(smsMessages);
        StreamPair validDataUsage = processor.processDataUsage(dataUsage);

        // Merge and process all streams
        processor.mergeStreams(validVoiceCalls, validSmsMessages, validDataUsage);

        try {
            // Wait for topics to be created
            waitForTopics(KafkaConfig.BOOTSTRAP_SERVERS,
                    Arrays.asList("voice-calls", "SMS-messages", "Data-session-usage"));

            // Execute the Flink job
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
