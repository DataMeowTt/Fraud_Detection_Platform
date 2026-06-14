package com.frauddetection.aggregator.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.common.model.DecisionStatus;
import com.frauddetection.common.model.FraudDecision;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

public class FraudAlertSink implements Sink<FraudDecision> {

    private static final String BROKERS = System.getenv().getOrDefault(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
    private static final String TOPIC   = "fraud-alerts";

    @SuppressWarnings("deprecation")
    @Override
    public SinkWriter<FraudDecision> createWriter(Sink.InitContext context) throws IOException {
        return new AlertWriter(BROKERS, TOPIC);
    }

    static class AlertWriter implements SinkWriter<FraudDecision> {

        private final KafkaProducer<String, String> producer;
        private final ObjectMapper                  mapper;
        private final String                        topic;

        AlertWriter(String brokers, String topic) {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      brokers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            this.producer = new KafkaProducer<>(props);
            this.mapper   = new ObjectMapper();
            this.topic    = topic;
        }

        @Override
        public void write(FraudDecision d, Context context) throws IOException {
            if (d.status == DecisionStatus.APPROVED) return;
            try {
                String payload = mapper.writeValueAsString(d);
                producer.send(new ProducerRecord<>(topic, d.accountId, payload));
            } catch (Exception e) {
                throw new IOException("Failed to send alert", e);
            }
        }

        @Override
        public void flush(boolean endOfInput) {
            producer.flush();
        }

        @Override
        public void close() {
            producer.close();
        }
    }
}
