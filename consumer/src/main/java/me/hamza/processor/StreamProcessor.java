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
    ///
    public DataStream<String> processVoiceCalls(DataStream<String> voiceCalls) {
        // First, parse and validate the records
        DataStream<VoiceCallRecord> parsedCalls = voiceCalls.map(new MapFunction<String, VoiceCallRecord>() {
            @Override
            public VoiceCallRecord map(String record) throws Exception {
                try {
                    return objectMapper.readValue(record, VoiceCallRecord.class);
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

        // Convert back to strings and send to Kafka
        validCalls.map(new MapFunction<VoiceCallRecord, String>() {
            @Override
            public String map(VoiceCallRecord record) throws Exception {
                System.out.println("Valid Voice Call with caller_id: " + record.getCallerId());
                return objectMapper.writeValueAsString(record);
            }
        })
                .name("Valid Voice Calls")
                .sinkTo(createKafkaSink("valid-voice-calls"));

        invalidCalls.map(new MapFunction<VoiceCallRecord, String>() {
            @Override
            public String map(VoiceCallRecord record) throws Exception {
                String reason = record == null ? "Parse Error" : "Missing Caller ID";
                System.out.println("Invalid Voice Call: " + reason);
                return record == null ? "{}" : objectMapper.writeValueAsString(record);
            }
        })
                .name("Invalid Voice Calls")
                .sinkTo(createKafkaSink("invalid-voice-calls"));

        // Return the valid calls stream for further processing
        return validCalls.map(new MapFunction<VoiceCallRecord, String>() {
            @Override
            public String map(VoiceCallRecord record) throws Exception {
                return objectMapper.writeValueAsString(record);
            }
        });
    }

    //////////////////////////////////////////////////////////////
    ///////////////////// SMS Messages Processor /////////////////
    //////////////////////////////////////////////////////////////

    public DataStream<String> processSmsMessages(DataStream<String> smsMessages) {
        // First, parse and validate the records
        DataStream<SMSRecord> parsedSms = smsMessages.map(new MapFunction<String, SMSRecord>() {
            @Override
            public SMSRecord map(String record) throws Exception {
                try {
                    return objectMapper.readValue(record, SMSRecord.class);
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
                return record != null && record.getSenderId() != null && record.getReceiverId() != null;
            }
        });

        DataStream<SMSRecord> invalidSms = parsedSms.filter(new FilterFunction<SMSRecord>() {
            @Override
            public boolean filter(SMSRecord record) {
                return record == null || record.getSenderId() == null || record.getReceiverId() == null;
            }
        });

        // Convert back to strings and send to Kafka
        validSms.map(new MapFunction<SMSRecord, String>() {
            @Override
            public String map(SMSRecord record) throws Exception {
                System.out.println("Valid SMS with sender_id: " + record.getSenderId() +
                        " and receiver_id: " + record.getReceiverId());
                return objectMapper.writeValueAsString(record);
            }
        })
                .name("Valid SMS Messages")
                .sinkTo(createKafkaSink("valid-sms-messages"));

        invalidSms.map(new MapFunction<SMSRecord, String>() {
            @Override
            public String map(SMSRecord record) throws Exception {
                String reason = record == null ? "Parse Error"
                        : (record.getSenderId() == null ? "Missing Sender ID" : "Missing Receiver ID");
                System.out.println("Invalid SMS: " + reason);
                return record == null ? "{}" : objectMapper.writeValueAsString(record);
            }
        })
                .name("Invalid SMS Messages")
                .sinkTo(createKafkaSink("invalid-sms-messages"));

        // Return the valid SMS stream for further processing
        return validSms.map(new MapFunction<SMSRecord, String>() {
            @Override
            public String map(SMSRecord record) throws Exception {
                return objectMapper.writeValueAsString(record);
            }
        });
    }

    //////////////////////////////////////////////////////////////
    ///////////////////// Data Usage Processor ///////////////////
    //////////////////////////////////////////////////////////////

    public DataStream<String> processDataUsage(DataStream<String> dataUsage) {
        // First, parse and validate the records
        DataStream<DataSessionRecord> parsedData = dataUsage.map(new MapFunction<String, DataSessionRecord>() {
            @Override
            public DataSessionRecord map(String record) throws Exception {
                try {
                    DataSessionRecord dataSessionRecord = objectMapper.readValue(record, DataSessionRecord.class);
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

        // Convert back to strings and send to Kafka
        validData.map(new MapFunction<DataSessionRecord, String>() {
            @Override
            public String map(DataSessionRecord record) throws Exception {

                System.out.println("Valid Data Session with user_id: " + record.getUserId() +
                        " and volume: " + record.getDataVolumeMb() + "MB");
                return objectMapper.writeValueAsString(record);
            }
        })
                .name("Valid Data Usage")
                .sinkTo(createKafkaSink("valid-data-usage"));

        invalidData.map(new MapFunction<DataSessionRecord, String>() {
            @Override
            public String map(DataSessionRecord record) throws Exception {
                String reason = record == null ? "Parse Error"
                        : (record.getUserId() == null ? "Missing User ID" : "Invalid Data Volume");
                System.out.println("Invalid Data Session: " + reason);
                return record == null ? "{}" : objectMapper.writeValueAsString(record);
            }
        })
                .name("Invalid Data Usage")
                .sinkTo(createKafkaSink("invalid-data-usage"));

        // Return the valid data stream for further processing
        return validData.map(new MapFunction<DataSessionRecord, String>() {
            @Override
            public String map(DataSessionRecord record) throws Exception {
                return objectMapper.writeValueAsString(record);
            }
        });
    }

    //////////////////////////////////////////////////////////////
    /////////////////////// Merge Streams ////////////////////////
    //////////////////////////////////////////////////////////////

    // public DataStream<String> mergeStreams(DataStream<String> voiceCalls,
    // DataStream<String> smsMessages,
    // DataStream<String> dataUsage) {
    // return voiceCalls.union(smsMessages, dataUsage)
    // .map(record -> {
    // System.out.println("Merged Record: " + record);
    // return record;
    // })
    // .name("Merged Stream Processor")
    // .sinkTo(createKafkaSink("merged-records"));
    // }
}