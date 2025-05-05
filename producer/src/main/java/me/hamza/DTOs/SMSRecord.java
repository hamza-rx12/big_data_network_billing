package me.hamza.DTOs;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SMSRecord {
    @JsonProperty("record_type")
    private final String recordType = "sms";

    @JsonProperty("timestamp")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'", timezone = "UTC")
    private String timestamp;

    @JsonProperty("sender_id")
    private String senderId;

    @JsonProperty("receiver_id")
    private String receiverId;

    @JsonProperty("cell_id")
    private String cellId;

    @JsonProperty("technology")
    private String technology;

    // Constructor
    public SMSRecord(String timestamp, String senderId, String receiverId, String cellId, String technology) {
        this.timestamp = timestamp;
        this.senderId = senderId;
        this.receiverId = receiverId;
        this.cellId = cellId;
        this.technology = technology;
    }

    // Getters
    public String getRecordType() {
        return recordType;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getSenderId() {
        return senderId;
    }

    public String getReceiverId() {
        return receiverId;
    }

    public String getCellId() {
        return cellId;
    }

    public String getTechnology() {
        return technology;
    }

    @Override
    public String toString() {
        return "SMSRecord [recordType=" + recordType + ", timestamp=" + timestamp + ", senderId=" + senderId
                + ", receiverId=" + receiverId + ", cellId=" + cellId + ", technology=" + technology + "]";
    }

}