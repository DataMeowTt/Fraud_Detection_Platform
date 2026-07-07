package com.frauddetection.aggregator;

import com.frauddetection.aggregator.pipeline.DecisionPipeline;
import com.frauddetection.aggregator.sink.ClickHouseSink;
import com.frauddetection.common.model.DlqRecord;
import com.frauddetection.common.model.FraudDecision;
import com.frauddetection.common.model.FraudRule;
import com.frauddetection.common.model.Transaction;
import com.frauddetection.common.serialization.DlqRecordSerializationSchema;
import com.frauddetection.common.serialization.FraudRuleDeserializer;
import com.frauddetection.common.serialization.TransactionDeserializer;
import com.frauddetection.common.util.EitherSplitFunction;
import com.frauddetection.rules.state.RuleStateDescriptor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Either;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class DecisionAggregatorJob {

    private static final String KAFKA_BROKERS  = System.getenv().getOrDefault(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
    private static final String CONSUMER_GROUP = "decision-aggregator";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);
        env.enableCheckpointing(60_000);
        env.getCheckpointConfig().setCheckpointingConsistencyMode(
                org.apache.flink.core.execution.CheckpointingMode.AT_LEAST_ONCE);

        KafkaSource<Either<Transaction, DlqRecord>> txSource = KafkaSource.<Either<Transaction, DlqRecord>>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics("transactions")
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new TransactionDeserializer())
                .build();

        KafkaSource<Either<FraudRule, DlqRecord>> rulesSource = KafkaSource.<Either<FraudRule, DlqRecord>>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics("rules-updates")
                .setGroupId(CONSUMER_GROUP + "-rules")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new FraudRuleDeserializer())
                .build();

        OutputTag<DlqRecord> txDlqTag   = new OutputTag<DlqRecord>("transactions-dlq") {};
        OutputTag<DlqRecord> ruleDlqTag = new OutputTag<DlqRecord>("rules-updates-dlq") {};

        SingleOutputStreamOperator<Transaction> transactionsRaw = env
                .fromSource(txSource, WatermarkStrategy.noWatermarks(), "kafka-transactions")
                .process(new EitherSplitFunction<>(txDlqTag))
                .name("split-transactions-dlq");

        DataStream<Transaction> transactions = transactionsRaw.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((tx, ts) -> parseEpochMs(tx.eventTime)));

        SingleOutputStreamOperator<FraudRule> rulesRaw = env
                .fromSource(rulesSource, WatermarkStrategy.noWatermarks(), "kafka-rules")
                .setParallelism(1)
                .process(new EitherSplitFunction<>(ruleDlqTag))
                .setParallelism(1)
                .name("split-rules-dlq");

        BroadcastStream<FraudRule> broadcastRules = rulesRaw.broadcast(RuleStateDescriptor.DESCRIPTOR);

        DataStream<FraudDecision> decisions = transactions
                .keyBy(tx -> tx.accountId)
                .connect(broadcastRules)
                .process(new DecisionPipeline());

        decisions.sinkTo(new ClickHouseSink()).name("clickhouse-sink");

        transactionsRaw.getSideOutput(txDlqTag)
                .sinkTo(dlqSink("transactions-dlq"))
                .name("transactions-dlq-sink");

        rulesRaw.getSideOutput(ruleDlqTag)
                .sinkTo(dlqSink("rules-updates-dlq"))
                .setParallelism(1)
                .name("rules-updates-dlq-sink");

        env.execute("DecisionAggregatorJob");
    }

    private static KafkaSink<DlqRecord> dlqSink(String topic) {
        return KafkaSink.<DlqRecord>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new DlqRecordSerializationSchema())
                        .build())
                .build();
    }

    private static long parseEpochMs(String isoTimestamp) {
        return java.time.Instant.parse(isoTimestamp).toEpochMilli();
    }
}
