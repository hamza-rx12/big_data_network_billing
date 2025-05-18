package me.hamza.DTOs;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DataSessionRecord {
    @JsonProperty("record_type")
    private final Integer recordType = 3;

    @JsonProperty("timestamp")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'", timezone = "UTC")
    private String timestamp;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("data_volume_mb")
    private Double dataVolumeMb;

    @JsonProperty("session_duration_sec")
    private Integer sessionDurationSec;

    @JsonProperty("cell_id")
    private String cellId;

    @JsonProperty("technology")
    private String technology;

    // Constructor
    public DataSessionRecord(String timestamp, String userId, double dataVolumeMb, int sessionDurationSec,
            String cellId, String technology) {
        this.timestamp = timestamp;
        this.userId = userId;
        this.dataVolumeMb = dataVolumeMb;
        this.sessionDurationSec = sessionDurationSec;
        this.cellId = cellId;
        this.technology = technology;
    }

    // Getters
    public Integer getRecordType() {
        return recordType;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getUserId() {
        return userId;
    }

    public double getDataVolumeMb() {
        return dataVolumeMb;
    }

    public int getSessionDurationSec() {
        return sessionDurationSec;
    }

    public String getCellId() {
        return cellId;
    }

    public String getTechnology() {
        return technology;
    }

    @Override
    public String toString() {
        return "DataSessionRecord [recordType=" + recordType + ", timestamp=" + timestamp + ", userId=" + userId
                + ", dataVolumeMb=" + dataVolumeMb + ", sessionDurationSec=" + sessionDurationSec + ", cellId=" + cellId
                + ", technology=" + technology + "]";
    }

}