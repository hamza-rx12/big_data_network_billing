package me.hamza.processor;

import java.io.Serializable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.hamza.DTOs.VoiceCallRecord;
import me.hamza.DTOs.SMSRecord;
import me.hamza.DTOs.DataSessionRecord;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import me.hamza.config.MongoConfig;

public class StreamProcessor implements Serializable {
    private static final long serialVersionUID = 1L;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String bootstrapServers;
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
            .withZone(ZoneOffset.UTC);

    public StreamProcessor(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    private String generateTimestamp() {
        return TIMESTAMP_FORMATTER.format(Instant.now());
    }

    private KafkaSink<String> createKafkaSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
    }

    //////////////////////////////////////////////////////////////
    //////////////////// Voice Calls Processor ///////////////////
    //////////////////////////////////////////////////////////////
    public class StreamPair {
        public final DataStream<String> valid;
        public final DataStream<String> invalid;

        public StreamPair(DataStream<String> valid, DataStream<String> invalid) {
            this.valid = valid;
            this.invalid = invalid;
        }
    }

    public StreamPair processVoiceCalls(DataStream<String> voiceCalls) {
        // First, parse and validate the records
        DataStream<VoiceCallRecord> parsedCalls = voiceCalls.map(new MapFunction<String, VoiceCallRecord>() {
            @Override
            public VoiceCallRecord map(String record) throws Exception {
                try {
                    VoiceCallRecord callRecord = objectMapper.readValue(record, VoiceCallRecord.class);
                    if (callRecord.getTimestamp() == null) {
                        callRecord.setTimestamp(generateTimestamp());
                    }
                    return callRecord;
                } catch (Exception e) {
                    System.err.println("Error parsing Voice Call record: " + e.getMessage());
                    return null;
                }
            }
        });

        // Split into valid and invalid records
        DataStream<VoiceCallRecord> validCalls = parsedCalls.filter(new FilterFunction<VoiceCallRecord>() {
            @Override
            public boolean filter(VoiceCallRecord record) {
                return record != null && record.getCallerId() != null;
            }
        });

        DataStream<VoiceCallRecord> invalidCalls = parsedCalls.filter(new FilterFunction<VoiceCallRecord>() {
            @Override
            public boolean filter(VoiceCallRecord record) {
                return record == null || record.getCallerId() == null;
            }
        });

        // Convert back to strings
        DataStream<String> validStream = validCalls.map(new MapFunction<VoiceCallRecord, String>() {
            @Override
            public String map(VoiceCallRecord record) throws Exception {
                System.out.println("Valid Voice Call with caller_id: " + record.getCallerId());
                return objectMapper.writeValueAsString(record);
            }
        }).name("Valid Voice Calls");

        DataStream<String> invalidStream = invalidCalls.map(new MapFunction<VoiceCallRecord, String>() {
            @Override
            public String map(VoiceCallRecord record) throws Exception {
                String reason = record == null ? "Parse Error" : "Missing Caller ID";
                System.out.println("Invalid Voice Call: " + reason);
                return record == null ? "{}" : objectMapper.writeValueAsString(record);
            }
        }).name("Invalid Voice Calls");

        return new StreamPair(validStream, invalidStream);
    }

    //////////////////////////////////////////////////////////////
    ///////////////////// SMS Messages Processor /////////////////
    //////////////////////////////////////////////////////////////
    public StreamPair processSmsMessages(DataStream<String> smsMessages) {
        // First, parse and validate the records
        DataStream<SMSRecord> parsedSms = smsMessages.map(new MapFunction<String, SMSRecord>() {
            @Override
            public SMSRecord map(String record) throws Exception {
                try {
                    SMSRecord smsRecord = objectMapper.readValue(record, SMSRecord.class);
                    if (smsRecord.getTimestamp() == null) {
                        smsRecord.setTimestamp(generateTimestamp());
                    }
                    return smsRecord;
                } catch (Exception e) {
                    System.err.println("Error parsing SMS record: " + e.getMessage());
                    return null;
                }
            }
        });

        // Split into valid and invalid records
        DataStream<SMSRecord> validSms = parsedSms.filter(new FilterFunction<SMSRecord>() {
            @Override
            public boolean filter(SMSRecord record) {
                return record != null && record.getSenderId() != null; // && record.getReceiverId() != null;
            }
        });

        DataStream<SMSRecord> invalidSms = parsedSms.filter(new FilterFunction<SMSRecord>() {
            @Override
            public boolean filter(SMSRecord record) {
                return record == null || record.getSenderId() == null; // || record.getReceiverId() == null;
            }
        });

        // Convert back to strings
        DataStream<String> validStream = validSms.map(new MapFunction<SMSRecord, String>() {
            @Override
            public String map(SMSRecord record) throws Exception {
                System.out.println("Valid SMS with sender_id: " + record.getSenderId() +
                        " and receiver_id: " + record.getReceiverId());
                return objectMapper.writeValueAsString(record);
            }
        }).name("Valid SMS Messages");

        DataStream<String> invalidStream = invalidSms.map(new MapFunction<SMSRecord, String>() {
            @Override
            public String map(SMSRecord record) throws Exception {
                String reason = record == null ? "Parse Error"
                        : (record.getSenderId() == null ? "Missing Sender ID" : "Missing Receiver ID");
                System.out.println("Invalid SMS: " + reason);
                return record == null ? "{}" : objectMapper.writeValueAsString(record);
            }
        }).name("Invalid SMS Messages");

        return new StreamPair(validStream, invalidStream);
    }

    //////////////////////////////////////////////////////////////
    ///////////////////// Data Usage Processor ///////////////////
    //////////////////////////////////////////////////////////////
    public StreamPair processDataUsage(DataStream<String> dataUsage) {
        // First, parse and validate the records
        DataStream<DataSessionRecord> parsedData = dataUsage.map(new MapFunction<String, DataSessionRecord>() {
            @Override
            public DataSessionRecord map(String record) throws Exception {
                try {
                    DataSessionRecord dataSessionRecord = objectMapper.readValue(record, DataSessionRecord.class);
                    if (dataSessionRecord.getTimestamp() == null) {
                        dataSessionRecord.setTimestamp(generateTimestamp());
                    }
                    // Format dataVolumeMb to 2 decimal places
                    double formattedVolume = Math.round(dataSessionRecord.getDataVolumeMb() * 100.0) / 100.0;
                    dataSessionRecord.setDataVolumeMb(formattedVolume);
                    return dataSessionRecord;
                } catch (Exception e) {
                    System.err.println("Error parsing Data Session record: " + e.getMessage());
                    return null;
                }
            }
        });

        // Split into valid and invalid records
        DataStream<DataSessionRecord> validData = parsedData.filter(new FilterFunction<DataSessionRecord>() {
            @Override
            public boolean filter(DataSessionRecord record) {
                return record != null && record.getUserId() != null && record.getDataVolumeMb() > 0;
            }
        });

        DataStream<DataSessionRecord> invalidData = parsedData.filter(new FilterFunction<DataSessionRecord>() {
            @Override
            public boolean filter(DataSessionRecord record) {
                return record == null || record.getUserId() == null || record.getDataVolumeMb() <= 0;
            }
        });

        // Convert back to strings
        DataStream<String> validStream = validData.map(new MapFunction<DataSessionRecord, String>() {
            @Override
            public String map(DataSessionRecord record) throws Exception {
                System.out.println("Valid Data Session with user_id: " + record.getUserId() +
                        " and volume: " + record.getDataVolumeMb() + "MB");
                return objectMapper.writeValueAsString(record);
            }
        }).name("Valid Data Usage");

        DataStream<String> invalidStream = invalidData.map(new MapFunction<DataSessionRecord, String>() {
            @Override
            public String map(DataSessionRecord record) throws Exception {
                String reason = record == null ? "Parse Error"
                        : (record.getUserId() == null ? "Missing User ID" : "Invalid Data Volume");
                System.out.println("Invalid Data Session: " + reason);
                return record == null ? "{}" : objectMapper.writeValueAsString(record);
            }
        }).name("Invalid Data Usage");

        return new StreamPair(validStream, invalidStream);
    }

    //////////////////////////////////////////////////////////////
    /////////////////////// Merge Streams ////////////////////////
    //////////////////////////////////////////////////////////////
    public void mergeStreams(StreamPair voiceCalls,
            StreamPair smsMessages,
            StreamPair dataUsage) {
        // Merge all valid streams
        DataStream<String> validStream = voiceCalls.valid.union(smsMessages.valid, dataUsage.valid)
                .map(record -> {
                    System.out.println("Valid Record: " + record);
                    return record;
                })
                .name("Valid Streams Processor");

        // Create separate streams for valid sinks
        DataStream<String> validKafkaStream = validStream.map(record -> record);
        DataStream<String> validMongoStream = validStream.map(record -> record);

        // Apply sinks to valid streams
        validKafkaStream.sinkTo(createKafkaSink("valid-records"));
        validMongoStream.sinkTo(MongoConfig.createMongoSink("valid_records"));

        // Merge all invalid streams
        DataStream<String> invalidStream = voiceCalls.invalid.union(smsMessages.invalid, dataUsage.invalid)
                .map(record -> {
                    System.out.println("Invalid Record: " + record);
                    return record;
                })
                .name("Invalid Streams Processor");

        // Create separate streams for invalid sinks
        DataStream<String> invalidKafkaStream = invalidStream.map(record -> record);
        DataStream<String> invalidMongoStream = invalidStream.map(record -> record);

        // Apply sinks to invalid streams
        invalidKafkaStream.sinkTo(createKafkaSink("invalid-records"));
        invalidMongoStream.sinkTo(MongoConfig.createMongoSink("invalid_records"));
    }
}