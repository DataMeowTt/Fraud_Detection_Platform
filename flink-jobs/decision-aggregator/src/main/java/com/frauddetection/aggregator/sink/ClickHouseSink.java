package com.frauddetection.aggregator.sink;

import com.frauddetection.common.model.FraudDecision;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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

        private static final int  BATCH_SIZE  = 5000;
        private static final long MAX_WAIT_MS = 2000;

        private final Connection        conn;
        private final PreparedStatement stmt;
        private final LinkedBlockingQueue<FraudDecision> queue = new LinkedBlockingQueue<>();
        private final AtomicLong enqueued  = new AtomicLong(0);
        private final AtomicLong committed = new AtomicLong(0);
        private volatile boolean   running    = true;
        private volatile boolean   forceFlush = false;
        private volatile IOException flushError = null;
        private final Thread flusher;

        ClickHouseWriter(String url) throws IOException {
            try {
                conn = DriverManager.getConnection(url);
                stmt = conn.prepareStatement(SQL);
            } catch (Exception e) {
                throw new IOException("Failed to connect to ClickHouse", e);
            }
            flusher = new Thread(this::flushLoop, "clickhouse-flusher");
            flusher.setDaemon(true);
            flusher.start();
        }

        private void flushLoop() {
            List<FraudDecision> batch = new ArrayList<>(BATCH_SIZE);
            long batchStartMs = 0;
            while (running || !queue.isEmpty()) {
                try {
                    FraudDecision d = queue.poll(50, TimeUnit.MILLISECONDS);
                    if (d != null) {
                        if (batch.isEmpty()) {
                            batchStartMs = System.currentTimeMillis();
                        }
                        batch.add(d);
                        queue.drainTo(batch, BATCH_SIZE - batch.size());
                    }
                    boolean timedOut = !batch.isEmpty() && (System.currentTimeMillis() - batchStartMs >= MAX_WAIT_MS);
                    if (!batch.isEmpty() && (batch.size() >= BATCH_SIZE || forceFlush || !running || timedOut)) {
                        executeBatch(batch);
                        committed.addAndGet(batch.size());
                        batch.clear();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    flushError = new IOException("ClickHouse batch flush failed", e);
                    break;
                }
            }
            if (!batch.isEmpty()) {
                try {
                    executeBatch(batch);
                    committed.addAndGet(batch.size());
                } catch (Exception e) {
                    if (flushError == null)
                        flushError = new IOException("ClickHouse final flush failed", e);
                }
            }
        }

        private void executeBatch(List<FraudDecision> batch) throws Exception {
            for (FraudDecision d : batch) {
                stmt.setString(1, d.transactionId);
                stmt.setString(2, d.accountId);
                stmt.setBigDecimal(3, java.math.BigDecimal.valueOf(d.amount));
                stmt.setString(4, d.ruleName);
                stmt.setString(5, d.cepPattern);
                if (d.mlScore != null) stmt.setFloat(6, d.mlScore);
                else                   stmt.setNull(6, java.sql.Types.FLOAT);
                stmt.setObject(7, java.time.OffsetDateTime.parse(d.producedAt).toLocalDateTime());
                stmt.setObject(8, java.time.OffsetDateTime.parse(d.decidedAt).toLocalDateTime());
                stmt.setString(9, d.status.name());
                stmt.addBatch();
            }
            stmt.executeBatch();
        }

        @Override
        public void write(FraudDecision d, Context context) throws IOException {
            if (flushError != null) throw flushError;
            queue.offer(d);
            enqueued.incrementAndGet();
        }

        @Override
        public void flush(boolean endOfInput) throws IOException {
            forceFlush = true;
            long target = enqueued.get();
            while (committed.get() < target && flushError == null) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            forceFlush = false;
            if (flushError != null) throw flushError;
        }

        @Override
        public void close() throws Exception {
            running = false;
            flusher.join(30_000);
            if (stmt != null) stmt.close();
            if (conn != null) conn.close();
        }
    }
}
