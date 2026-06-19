package com.frauddetection.aggregator;

import com.frauddetection.aggregator.pipeline.DecisionPipeline;
import com.frauddetection.aggregator.sink.ClickHouseSink;
import com.frauddetection.common.model.FraudDecision;
import com.frauddetection.common.model.Transaction;
import com.frauddetection.common.serialization.TransactionDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class DecisionAggregatorJob {

    private static final String KAFKA_BROKERS  = System.getenv().getOrDefault(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
    private static final String CONSUMER_GROUP = "decision-aggregator";
    private static final String INPUT_TOPIC    = "transactions";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.enableCheckpointing(60_000);

        KafkaSource<Transaction> kafkaSource = KafkaSource.<Transaction>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TransactionDeserializer())
                .build();

        DataStream<Transaction> transactions = env.fromSource(
                kafkaSource,
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((tx, ts) -> parseEpochMs(tx.eventTime)),
                "kafka-transactions"
        );

        DataStream<FraudDecision> decisions = transactions
                .keyBy(tx -> tx.accountId)
                .map(new DecisionPipeline());

        decisions.sinkTo(new ClickHouseSink()).name("clickhouse-sink");

        env.execute("DecisionAggregatorJob");
    }

    private static long parseEpochMs(String isoTimestamp) {
        return java.time.Instant.parse(isoTimestamp).toEpochMilli();
    }
}
