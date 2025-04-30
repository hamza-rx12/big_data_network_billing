package me.hamza.DTOs;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

// import java.time.Instant;

public class VoiceCallRecord {
    @JsonProperty("record_type")
    private final String recordType = "voice";

    @JsonProperty("timestamp")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss'Z'", timezone = "UTC")
    private String timestamp;

    @JsonProperty("caller_id")
    private String callerId;

    @JsonProperty("callee_id")
    private String calleeId;

    @JsonProperty("duration_sec")
    private int durationSec;

    @JsonProperty("cell_id")
    private String cellId;

    @JsonProperty("technology")
    private String technology;

    // Constructor, getters, and setters (or use Lombok @Data)
    public VoiceCallRecord(String timestamp, String callerId, String calleeId, int durationSec, String cellId,
            String technology) {
        this.timestamp = timestamp;
        this.callerId = callerId;
        this.calleeId = calleeId;
        this.durationSec = durationSec;
        this.cellId = cellId;
        this.technology = technology;
    }

    // Getters (needed for Jackson serialization)
    public String getRecordType() {
        return recordType;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getCallerId() {
        return callerId;
    }

    public String getCalleeId() {
        return calleeId;
    }

    public int getDurationSec() {
        return durationSec;
    }

    public String getCellId() {
        return cellId;
    }

    public String getTechnology() {
        return technology;
    }
}
