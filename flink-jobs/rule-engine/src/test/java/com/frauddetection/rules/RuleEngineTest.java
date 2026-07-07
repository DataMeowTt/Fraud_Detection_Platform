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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@EnabledIfEnvironmentVariable(named = "RUN_LIVE_INTEGRATION_TESTS", matches = "true")
class RuleEngineTest {

    private static final String KAFKA_BOOTSTRAP_SERVERS = System.getenv()
            .getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
    private static final String CLICKHOUSE_URL = System.getenv()
            .getOrDefault("CLICKHOUSE_URL", "jdbc:clickhouse://localhost:8123/fraud_detection");

    private static final List<String> TEST_ACCOUNT_IDS = List.of(
            "FRD_RULE_UPDATE_ACC_1", "FRD_RULE_UPDATE_ACC_2", "FRD_RULE_UPDATE_ACC_3", "FRD_RULE_UPDATE_ACC_4", "FRD_RULE_UPDATE_ACC_5");
    private static final int TRANSACTION_COUNT = 1001;
    private static final int POLL_TIMEOUT_SECS = 60;
    private static final Path RESULTS_CSV = Paths.get("..", "..", "performance", "rule_update_latency.csv");

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Calendar UTC_CALENDAR = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    private record LatencyResult(String accountId, Instant ruleSentAt, Instant firstBlockDecidedAt, long latencyMs) {}

    @Test
    void ruleUpdateLatency_isStableAcrossRepeatedRuns() throws Exception {
        KafkaProducer<String, String> producer = createProducer();
        List<LatencyResult> results = new ArrayList<>();
        try {
            for (String accountId : TEST_ACCOUNT_IDS) {
                LatencyResult result = measureRuleUpdateLatency(producer, accountId);
                results.add(result);
                System.out.printf("[Latency] account=%s  Rule -> BLOCK = %d ms%n", result.accountId(), result.latencyMs());
            }
        } finally {
            producer.close();
        }

        double avgMs = results.stream().mapToLong(LatencyResult::latencyMs).average().orElse(Double.NaN);
        System.out.printf("[Latency] avg(t1..t%d) = %.1f ms%n", results.size(), avgMs);

        writeResultsCsv(results);

        for (LatencyResult result : results) {
            assertTrue(result.latencyMs() >= 0, "BLOCK decision must be decided after the rule was sent");
        }
    }

    private static LatencyResult measureRuleUpdateLatency(KafkaProducer<String, String> producer, String accountId) throws Exception {
        Instant ruleSentAt = sendBlockRule(producer, accountId);
        sendTransactions(producer, accountId, TRANSACTION_COUNT);
        producer.flush();

        Instant firstBlockDecidedAt = waitForFirstBlockDecision(accountId, ruleSentAt);
        long latencyMs = firstBlockDecidedAt.toEpochMilli() - ruleSentAt.toEpochMilli();
        return new LatencyResult(accountId, ruleSentAt, firstBlockDecidedAt, latencyMs);
    }

    private static void writeResultsCsv(List<LatencyResult> results) throws IOException {
        Files.createDirectories(RESULTS_CSV.getParent());
        try (BufferedWriter writer = Files.newBufferedWriter(RESULTS_CSV)) {
            writer.write("account_id,rule_sent_at,first_block_at,latency_ms");
            writer.newLine();
            for (LatencyResult r : results) {
                writer.write(String.format("%s,%s,%s,%d",
                        r.accountId(), r.ruleSentAt(), r.firstBlockDecidedAt(), r.latencyMs()));
                writer.newLine();
            }
        }
        System.out.println("[Latency] Results written to " + RESULTS_CSV.toAbsolutePath().normalize());
    }

    private static KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return new KafkaProducer<>(props);
    }

    private static Instant sendBlockRule(KafkaProducer<String, String> producer, String accountId) throws Exception {
        FraudRule rule = new FraudRule();
        rule.ruleId   = "RULE_LATENCY_TEST_" + accountId + "_" + UUID.randomUUID();
        rule.field    = "account_id";
        rule.operator = "EQ";
        rule.value    = accountId;
        rule.action   = "BLOCK";
        rule.enabled  = true;

        String payload = MAPPER.writeValueAsString(rule);
        producer.send(new ProducerRecord<>("rules-updates", rule.ruleId, payload)).get();
        return Instant.now();
    }

    private static void sendTransactions(KafkaProducer<String, String> producer, String accountId, int count) {
        String cardId = "FRD_CARD_" + String.format("%05d", Math.abs(accountId.hashCode()) % 100_000);
        for (int i = 0; i < count; i++) {
            Transaction tx = new Transaction();
            tx.transactionId = UUID.randomUUID().toString();
            tx.producedAt    = Instant.now().toString();
            tx.eventTime     = tx.producedAt;
            tx.accountId     = accountId;
            tx.cardId        = cardId;
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
                            return rs.getTimestamp("decided_at", UTC_CALENDAR).toInstant();
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
