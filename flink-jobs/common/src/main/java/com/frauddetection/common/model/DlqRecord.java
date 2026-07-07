package com.frauddetection.common.model;

import java.io.Serializable;

public class DlqRecord implements Serializable {

    public String sourceTopic;
    public int    partition;
    public long   offset;
    public String rawPayload;
    public String errorMessage;
    public String failedAt;

    public DlqRecord() {}

    public DlqRecord(String sourceTopic, int partition, long offset, String rawPayload, String errorMessage) {
        this.sourceTopic  = sourceTopic;
        this.partition    = partition;
        this.offset       = offset;
        this.rawPayload   = rawPayload;
        this.errorMessage = errorMessage;
        this.failedAt     = java.time.Instant.now().toString();
    }
}
