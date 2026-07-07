package com.frauddetection.common.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.common.model.DlqRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class DlqRecordSerializationSchema implements SerializationSchema<DlqRecord> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(DlqRecord record) {
        try {
            return MAPPER.writeValueAsBytes(record);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize DlqRecord", e);
        }
    }
}
