package me.hamza.DTOs;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SMSRecord {
    @JsonProperty("record_type")
    private final Integer recordType = 2;

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

    public SMSRecord() {
    }

    // Getters
    public Integer getRecordType() {
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

    // Setters
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public void setSenderId(String senderId) {
        this.senderId = senderId;
    }

    public void setReceiverId(String receiverId) {
        this.receiverId = receiverId;
    }

    public void setCellId(String cellId) {
        this.cellId = cellId;
    }

    public void setTechnology(String technology) {
        this.technology = technology;
    }

    @Override
    public String toString() {
        return "SMSRecord [recordType=" + recordType + ", timestamp=" + timestamp + ", senderId=" + senderId
                + ", receiverId=" + receiverId + ", cellId=" + cellId + ", technology=" + technology + "]";
    }

}