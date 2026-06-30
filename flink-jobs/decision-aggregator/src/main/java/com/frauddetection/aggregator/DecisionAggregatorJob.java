package com.frauddetection.aggregator;

import com.frauddetection.aggregator.pipeline.DecisionPipeline;
import com.frauddetection.aggregator.sink.ClickHouseSink;
import com.frauddetection.common.model.FraudDecision;
import com.frauddetection.common.model.FraudRule;
import com.frauddetection.common.model.Transaction;
import com.frauddetection.common.serialization.FraudRuleDeserializer;
import com.frauddetection.common.serialization.TransactionDeserializer;
import com.frauddetection.rules.state.RuleStateDescriptor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class DecisionAggregatorJob {

    private static final String KAFKA_BROKERS  = System.getenv().getOrDefault(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
    private static final String CONSUMER_GROUP = "decision-aggregator";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        env.enableCheckpointing(600_000);
        env.getCheckpointConfig().setCheckpointingConsistencyMode(
                org.apache.flink.core.execution.CheckpointingMode.AT_LEAST_ONCE);

        KafkaSource<Transaction> txSource = KafkaSource.<Transaction>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics("transactions")
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TransactionDeserializer())
                .build();

        KafkaSource<FraudRule> rulesSource = KafkaSource.<FraudRule>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics("rules-updates")
                .setGroupId(CONSUMER_GROUP + "-rules")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new FraudRuleDeserializer())
                .build();

        DataStream<Transaction> transactions = env.fromSource(
                txSource,
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((tx, ts) -> parseEpochMs(tx.eventTime)),
                "kafka-transactions"
        );

        BroadcastStream<FraudRule> broadcastRules = env
                .fromSource(rulesSource, WatermarkStrategy.noWatermarks(), "kafka-rules")
                .setParallelism(1)
                .broadcast(RuleStateDescriptor.DESCRIPTOR);

        DataStream<FraudDecision> decisions = transactions
                .keyBy(tx -> tx.accountId)
                .connect(broadcastRules)
                .process(new DecisionPipeline());

        decisions.sinkTo(new ClickHouseSink()).name("clickhouse-sink");

        env.execute("DecisionAggregatorJob");
    }

    private static long parseEpochMs(String isoTimestamp) {
        return java.time.Instant.parse(isoTimestamp).toEpochMilli();
    }
}
