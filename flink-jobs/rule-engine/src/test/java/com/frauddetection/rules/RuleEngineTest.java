package com.frauddetection.rules;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.frauddetection.common.model.FraudRule;
import com.frauddetection.common.model.Transaction;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@EnabledIfEnvironmentVariable(named = "RUN_LIVE_INTEGRATION_TESTS", matches = "true")
class RuleEngineTest {

    private static final String KAFKA_BOOTSTRAP_SERVERS = System.getenv()
            .getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
    private static final String CLICKHOUSE_URL = System.getenv()
            .getOrDefault("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/fraud_detection");

    private static final String TEST_ACCOUNT_ID   = "1"; 
    private static final int    TRANSACTION_COUNT  = 5200;
    private static final int    POLL_TIMEOUT_SECS  = 60;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void ruleUpdateLatency_blockTakesEffectAfterRuleIsBroadcast() throws Exception {
        KafkaProducer<String, String> producer = createProducer();
        try {
            Instant ruleSentAt = sendBlockRule(producer);
            sendTransactions(producer, TEST_ACCOUNT_ID, TRANSACTION_COUNT);
            producer.flush();

            Instant firstBlockDecidedAt = waitForFirstBlockDecision(TEST_ACCOUNT_ID, ruleSentAt);

            long latencyMs = firstBlockDecidedAt.toEpochMilli() - ruleSentAt.toEpochMilli();
            System.out.printf("[Latency] Rule produced_at : %s%n", ruleSentAt);
            System.out.printf("[Latency] First BLOCK at   : %s%n", firstBlockDecidedAt);
            System.out.printf("[Latency] Rule -> BLOCK     : %d ms%n", latencyMs);

            assertTrue(latencyMs >= 0, "BLOCK decision must be decided after the rule was sent");
        } finally {
            producer.close();
        }
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return new KafkaProducer<>(props);
    }

    private static Instant sendBlockRule(KafkaProducer<String, String> producer) throws Exception {
        FraudRule rule = new FraudRule();
        rule.ruleId   = "RULE_LATENCY_TEST_" + UUID.randomUUID();
        rule.field    = "account_id";
        rule.operator = "EQ";
        rule.value    = TEST_ACCOUNT_ID;
        rule.action   = "BLOCK";
        rule.enabled  = true;

        String payload = MAPPER.writeValueAsString(rule);
        producer.send(new ProducerRecord<>("rules-updates", rule.ruleId, payload)).get();
        return Instant.now();
    }

    private static void sendTransactions(KafkaProducer<String, String> producer, String accountId, int count) {
        for (int i = 0; i < count; i++) {
            Transaction tx = new Transaction();
            tx.transactionId = "RULE_LATENCY_TEST_TX_" + i + "_" + UUID.randomUUID();
            tx.producedAt    = Instant.now().toString();
            tx.eventTime     = tx.producedAt;
            tx.accountId     = accountId;
            tx.cardId        = "RULE_LATENCY_TEST_CARD";
            tx.amount        = 100_000;
            tx.channel       = "ONLINE";
            tx.locationName  = "Hanoi";
            tx.status        = "Approved";

            try {
                String payload = MAPPER.writeValueAsString(tx);
                producer.send(new ProducerRecord<>("transactions", tx.accountId, payload));
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize/send transaction " + tx.transactionId, e);
            }
        }
    }

    private static Instant waitForFirstBlockDecision(String accountId, Instant sentAfter) throws Exception {
        String sql = """
                SELECT decided_at
                FROM fraud_detection.transactions
                WHERE account_id = ?
                  AND decision = 'BLOCK'
                  AND produced_at >= ?
                ORDER BY decided_at ASC
                LIMIT 1
                """;

        try (Connection conn = DriverManager.getConnection(CLICKHOUSE_URL)) {
            for (int attempt = 0; attempt < POLL_TIMEOUT_SECS; attempt++) {
                try (PreparedStatement stmt = conn.prepareStatement(sql)) {
                    stmt.setString(1, accountId);
                    stmt.setTimestamp(2, Timestamp.from(sentAfter));
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            return rs.getTimestamp("decided_at").toInstant();
                        }
                    }
                }
                Thread.sleep(1000);
            }
        }
        fail("No BLOCK decision found for account " + accountId + " within " + POLL_TIMEOUT_SECS + "s");
        throw new IllegalStateException("unreachable");
    }
}
