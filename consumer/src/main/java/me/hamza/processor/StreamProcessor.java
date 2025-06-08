package me.hamza.processor;

import java.io.Serializable;

import org.apache.flink.streaming.api.datastream.DataStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import me.hamza.DTOs.VoiceCallRecord;
import me.hamza.DTOs.SMSRecord;
import me.hamza.DTOs.DataSessionRecord;
import org.apache.flink.api.common.functions.MapFunction;

public class StreamProcessor implements Serializable {

    private static final long serialVersionUID = 1L;
    private final ObjectMapper objectMapper = new ObjectMapper();

    //////////////////////////////////////////////////////////////
    //////////////////// Voice Calls Processor ///////////////////
    //////////////////////////////////////////////////////////////
    ///
    public DataStream<String> processVoiceCalls(DataStream<String> voiceCalls) {
        return voiceCalls.map(new MapFunction<String, String>() {
            @Override
            public String map(String record) throws Exception {
                try {
                    VoiceCallRecord voiceCall = objectMapper.readValue(record, VoiceCallRecord.class);
                    if (voiceCall.getCallerId() != null) {
                        System.out.println("Valid Voice Call with caller_id: " + voiceCall.getCallerId());
                        return record;
                    } else {
                        System.out.println("Skipping Voice Call with null caller_id");
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

    //////////////////////////////////////////////////////////////
    ///////////////////// SMS Messages Processor /////////////////
    //////////////////////////////////////////////////////////////

    public DataStream<String> processSmsMessages(DataStream<String> smsMessages) {
        return smsMessages.map(new MapFunction<String, String>() {
            @Override
            public String map(String record) throws Exception {
                try {
                    SMSRecord sms = objectMapper.readValue(record, SMSRecord.class);
                    if (sms.getSenderId() != null && sms.getReceiverId() != null) {
                        System.out.println("Valid SMS with sender_id: " + sms.getSenderId() +
                                " and receiver_id: " + sms.getReceiverId());
                        return record;
                    } else {
                        System.out.println("Skipping SMS with null sender_id or receiver_id");
                        return null;
                    }
                } catch (Exception e) {
                    System.err.println("Error processing SMS record: " + e.getMessage());
                    return null;
                }
            }
        })
                .filter(record -> record != null)
                .name("Processed SMS Messages");
    }

    //////////////////////////////////////////////////////////////
    ///////////////////// Data Usage Processor ///////////////////
    //////////////////////////////////////////////////////////////

    public DataStream<String> processDataUsage(DataStream<String> dataUsage) {
        return dataUsage.map(new MapFunction<String, String>() {
            @Override
            public String map(String record) throws Exception {
                try {
                    DataSessionRecord dataSession = objectMapper.readValue(record, DataSessionRecord.class);
                    if (dataSession.getUserId() != null && dataSession.getDataVolumeMb() > 0) {
                        System.out.println("Valid Data Session with user_id: " + dataSession.getUserId() +
                                " and volume: " + dataSession.getDataVolumeMb() + "MB");
                        return record;
                    } else {
                        System.out.println("Skipping Data Session with null user_id or invalid volume");
                        return null;
                    }
                } catch (Exception e) {
                    System.err.println("Error processing Data Session record: " + e.getMessage());
                    return null;
                }
            }
        })
                .filter(record -> record != null)
                .name("Processed Data Usage");
    }

    //////////////////////////////////////////////////////////////
    /////////////////////// Merge Streams ////////////////////////
    //////////////////////////////////////////////////////////////

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