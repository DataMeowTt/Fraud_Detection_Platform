package com.frauddetection.common.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.common.model.FraudDecision;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class FraudDecisionDeserializer implements DeserializationSchema<FraudDecision> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public FraudDecision deserialize(byte[] message) throws IOException {
        return MAPPER.readValue(message, FraudDecision.class);
    }

    @Override
    public boolean isEndOfStream(FraudDecision nextElement) {
        return false;
    }

    @Override
    public TypeInformation<FraudDecision> getProducedType() {
        return TypeInformation.of(FraudDecision.class);
    }
}
