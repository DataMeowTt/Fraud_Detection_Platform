package com.frauddetection.common.serialization;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.common.model.DlqRecord;
import com.frauddetection.common.model.Transaction;
import com.frauddetection.common.validation.SchemaValidator;
import com.networknt.schema.ValidationMessage;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;

public class TransactionDeserializer implements KafkaRecordDeserializationSchema<Either<Transaction, DlqRecord>> {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SCHEMA_PATH = System.getenv().getOrDefault(
            "TRANSACTION_SCHEMA_PATH", "/opt/flink/schemas/transaction-schema.json");

    private transient SchemaValidator validator;

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        validator = new SchemaValidator(SCHEMA_PATH);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Either<Transaction, DlqRecord>> out) {
        try {
            JsonNode node = MAPPER.readTree(record.value());
            Set<ValidationMessage> errors = validator.validate(node);
            if (!errors.isEmpty()) {
                out.collect(Either.Right(toDlq(record, describe(errors))));
                return;
            }
            Transaction tx = MAPPER.treeToValue(node, Transaction.class);
            out.collect(Either.Left(tx));
        } catch (Exception e) {
            out.collect(Either.Right(toDlq(record, e.getMessage())));
        }
    }

    private static String describe(Set<ValidationMessage> errors) {
        return errors.stream().map(ValidationMessage::getMessage).collect(Collectors.joining("; "));
    }

    private static DlqRecord toDlq(ConsumerRecord<byte[], byte[]> record, String errorMessage) {
        return new DlqRecord(
                record.topic(), record.partition(), record.offset(),
                new String(record.value(), StandardCharsets.UTF_8), errorMessage);
    }

    @Override
    public TypeInformation<Either<Transaction, DlqRecord>> getProducedType() {
        return Types.EITHER(TypeInformation.of(Transaction.class), TypeInformation.of(DlqRecord.class));
    }
}
