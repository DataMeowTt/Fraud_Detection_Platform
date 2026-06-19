package com.frauddetection.common.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.common.model.FraudRule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class FraudRuleDeserializer implements DeserializationSchema<FraudRule> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public FraudRule deserialize(byte[] message) throws IOException {
        return MAPPER.readValue(message, FraudRule.class);
    }

    @Override
    public boolean isEndOfStream(FraudRule nextElement) {
        return false;
    }

    @Override
    public TypeInformation<FraudRule> getProducedType() {
        return TypeInformation.of(FraudRule.class);
    }
}
