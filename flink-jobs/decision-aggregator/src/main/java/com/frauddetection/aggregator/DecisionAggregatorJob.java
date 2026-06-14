package com.frauddetection.aggregator;

import com.frauddetection.common.model.FraudDecision;
import com.frauddetection.common.serialization.FraudDecisionDeserializer;
import com.frauddetection.aggregator.sink.ClickHouseSink;
import com.frauddetection.aggregator.strategy.DecisionStrategy;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DecisionAggregatorJob {

    private static final String KAFKA_BROKERS  = System.getenv().getOrDefault(
            "KAFKA_BOOTSTRAP_SERVERS", "localhost:29092");
    private static final String CONSUMER_GROUP = "decision-aggregator";
    private static final String INPUT_TOPIC    = "rule-decisions";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60_000);

        KafkaSource<FraudDecision> kafkaSource = KafkaSource.<FraudDecision>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId(CONSUMER_GROUP)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new FraudDecisionDeserializer())
                .build();

        DataStream<FraudDecision> ruleDecisions = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-rule-decisions"
        );

        // Final decision — priority: Rules > CEP > ML
        DataStream<FraudDecision> finalDecisions = ruleDecisions
                .map(new DecisionStrategy());

        finalDecisions.sinkTo(new ClickHouseSink()).name("clickhouse-sink");

        env.execute("DecisionAggregatorJob");
    }
}
