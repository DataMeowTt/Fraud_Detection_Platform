package com.frauddetection.common.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.common.model.FraudDecision;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class FraudDecisionSerializer implements SerializationSchema<FraudDecision> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(FraudDecision decision) {
        try {
            return MAPPER.writeValueAsBytes(decision);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize FraudDecision", e);
        }
    }
}
