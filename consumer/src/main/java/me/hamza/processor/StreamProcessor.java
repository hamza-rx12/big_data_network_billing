package me.hamza.processor;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamProcessor {
    private final StreamExecutionEnvironment env;

    public StreamProcessor(StreamExecutionEnvironment env) {
        this.env = env;
    }

    public DataStream<String> processVoiceCalls(DataStream<String> voiceCalls) {
        return voiceCalls.map(record -> {
            String processedRecord = "Processed Voice Call: " + record;
            System.out.println(processedRecord);
            return processedRecord;
        }).name("Processed Voice Calls");
    }

    public DataStream<String> processSmsMessages(DataStream<String> smsMessages) {
        return smsMessages.map(record -> {
            String processedRecord = record.toUpperCase();
            System.out.println("Processed SMS Message: " + processedRecord);
            return processedRecord;
        }).name("Processed SMS Messages");
    }

    public DataStream<String> processDataUsage(DataStream<String> dataUsage) {
        return dataUsage.map(record -> {
            String processedRecord = record + " [Data Usage Processed]";
            System.out.println(processedRecord);
            return processedRecord;
        }).name("Processed Data Usage");
    }

    public DataStream<String> mergeStreams(DataStream<String> voiceCalls,
            DataStream<String> smsMessages,
            DataStream<String> dataUsage) {
        return voiceCalls.union(smsMessages, dataUsage)
                .map(record -> {
                    System.out.println("Merged Record: " + record);
                    return record;
                }).name("Merged Stream Processor");
    }
}