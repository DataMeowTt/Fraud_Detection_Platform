package com.frauddetection.common.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.common.model.Transaction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class TransactionDeserializer implements DeserializationSchema<Transaction> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public Transaction deserialize(byte[] message) throws IOException {
        return MAPPER.readValue(message, Transaction.class);
    }

    @Override
    public boolean isEndOfStream(Transaction nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}
