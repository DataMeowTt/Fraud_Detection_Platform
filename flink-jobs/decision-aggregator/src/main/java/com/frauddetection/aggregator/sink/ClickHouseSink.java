package com.frauddetection.aggregator.sink;

import com.frauddetection.common.model.FraudDecision;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class ClickHouseSink implements Sink<FraudDecision> {

    private static final String URL = System.getenv().getOrDefault(
            "CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/fraud_detection");

    @Override
    @SuppressWarnings("deprecation")
    public SinkWriter<FraudDecision> createWriter(Sink.InitContext context) throws IOException {
        return new ClickHouseWriter(URL);
    }

    static class ClickHouseWriter implements SinkWriter<FraudDecision> {

        private static final String SQL = """
                INSERT INTO fraud_detection.transactions
                (transaction_id, account_id, amount, rule_hit, cep_pattern, ml_score, produced_at, decided_at, decision)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """;

        private static final int BATCH_SIZE = 1000;

        private final Connection        conn;
        private final PreparedStatement stmt;
        private int pendingCount = 0;

        ClickHouseWriter(String url) throws IOException {
            try {
                conn = DriverManager.getConnection(url);
                stmt = conn.prepareStatement(SQL);
            } catch (Exception e) {
                throw new IOException("Failed to connect to ClickHouse", e);
            }
        }

        @Override
        public void write(FraudDecision d, Context context) throws IOException {
            try {
                stmt.setString(1, d.transactionId);
                stmt.setString(2, d.accountId);
                stmt.setBigDecimal(3, java.math.BigDecimal.valueOf(d.amount));
                stmt.setString(4, d.ruleName);
                stmt.setString(5, d.cepPattern);
                if (d.mlScore != null) stmt.setFloat(6, d.mlScore);
                else                   stmt.setNull(6, java.sql.Types.FLOAT);
                stmt.setTimestamp(7, java.sql.Timestamp.from(java.time.OffsetDateTime.parse(d.producedAt).toInstant()));
                stmt.setTimestamp(8, java.sql.Timestamp.from(java.time.OffsetDateTime.parse(d.decidedAt).toInstant()));
                stmt.setString(9, d.status.name());
                stmt.addBatch();
                pendingCount++;
                if (pendingCount >= BATCH_SIZE) {
                    flush(false);
                }
            } catch (Exception e) {
                throw new IOException("ClickHouse insert failed", e);
            }
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            if (pendingCount == 0) return;
            try {
                stmt.executeBatch();
                pendingCount = 0;
            } catch (Exception e) {
                throw new IOException("ClickHouse batch flush failed", e);
            }
        }

        @Override
        public void close() throws Exception {
            try {
                flush(true);
            } finally {
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            }
        }
    }
}
