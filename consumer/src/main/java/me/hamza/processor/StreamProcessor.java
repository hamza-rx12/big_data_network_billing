package me.hamza.processor;

import java.io.Serializable;

import org.apache.flink.streaming.api.datastream.DataStream;
// import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.hamza.DTOs.VoiceCallRecord;
import org.apache.flink.api.common.functions.MapFunction;

public class StreamProcessor implements Serializable {
    private static final long serialVersionUID = 1L;
    // private final StreamExecutionEnvironment env;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // public StreamProcessor(StreamExecutionEnvironment env) {
    // this.env = env;
    // }

    public DataStream<String> processVoiceCalls(DataStream<String> voiceCalls) {
        return voiceCalls.map(new MapFunction<String, String>() {
            @Override
            public String map(String record) throws Exception {
                try {
                    // Deserialize JSON into VoiceCallRecord
                    VoiceCallRecord voiceCall = objectMapper.readValue(record, VoiceCallRecord.class);

                    // Check if user_id is not null
                    if (voiceCall.getCallerId() != null) {
                        System.out.println("Valid Voice Call with user_id: " + voiceCall.getCallerId());
                        return record; // Return original record if valid
                    } else {
                        System.out.println("Skipping Voice Call with null user_id");
                        return null;
                    }
                } catch (Exception e) {
                    System.err.println("Error processing Voice Call record: " + e.getMessage());
                    return null;
                }
            }
        })
                .filter(record -> record != null)
                .name("Processed Voice Calls");
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