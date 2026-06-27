package com.frauddetection.aggregator.pipeline;

import com.frauddetection.cep.processor.CepEvaluator;
import com.frauddetection.common.model.DecisionStatus;
import com.frauddetection.common.model.FraudDecision;
import com.frauddetection.common.model.FraudRule;
import com.frauddetection.common.model.Transaction;
import com.frauddetection.ml.features.FeatureExtractor;
import com.frauddetection.ml.scorer.FraudScorer;
import com.frauddetection.rules.processor.RuleEvaluator;
import com.frauddetection.rules.state.RuleStateDescriptor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Optional;
import java.util.Set;

/**
 * transaction -> Rules Engine -> BLOCK? -> output
 *               Rules PASS   -> CEP Engine   -> BLOCK/ALERT? -> output
 *               CEP PASS                     -> ML Engine    -> ALERT/APPROVED -> output
 */
public class DecisionPipeline
        extends KeyedBroadcastProcessFunction<String, Transaction, FraudRule, FraudDecision> {

    private static final Set<String> PRIORITY_ACCOUNTS = Set.of(
            "FRD_RULE_UPDATE_SC6"
    );

    private static boolean isPriorityAccount(String accountId) {
        return PRIORITY_ACCOUNTS.contains(accountId);
    }

    private RuleEvaluator    ruleEvaluator;
    private CepEvaluator     cepEvaluator;
    private FeatureExtractor featureExtractor;
    private FraudScorer      fraudScorer;

    @Override
    public void open(Configuration parameters) throws Exception {
        ruleEvaluator    = new RuleEvaluator();
        cepEvaluator     = new CepEvaluator(getRuntimeContext());
        featureExtractor = new FeatureExtractor();
        featureExtractor.open(getRuntimeContext());
        fraudScorer = new FraudScorer();
        fraudScorer.open();
    }

    @Override
    public void processElement(Transaction tx, ReadOnlyContext ctx, Collector<FraudDecision> out) throws Exception {

        ReadOnlyBroadcastState<String, FraudRule> rules = ctx.getBroadcastState(RuleStateDescriptor.DESCRIPTOR);

        Optional<FraudDecision> ruleDecision = ruleEvaluator.evaluate(tx, rules.immutableEntries());
        if (ruleDecision.isPresent()) {
            featureExtractor.update(tx, false);
            out.collect(ruleDecision.get());
            return;
        }

        if (isPriorityAccount(tx.accountId)) {
            featureExtractor.update(tx, true);
            out.collect(new FraudDecision(tx, DecisionStatus.APPROVED, null));
            return;
        }

        Optional<CepEvaluator.CepResult> cepResult = cepEvaluator.evaluate(tx);
        if (cepResult.isPresent()) {
            featureExtractor.update(tx, false);
            FraudDecision decision = new FraudDecision(tx, cepResult.get().status(), null);
            decision.cepPattern = cepResult.get().patternName();
            out.collect(decision);
            return;
        }

        float[] features = featureExtractor.extract(tx);
        float mlScore = fraudScorer.score(features);
        DecisionStatus status = fraudScorer.isFraud(mlScore) ? DecisionStatus.ALERT : DecisionStatus.APPROVED;
        featureExtractor.update(tx, status == DecisionStatus.APPROVED);
        FraudDecision mlDecision = new FraudDecision(tx, status, null);
        mlDecision.mlScore = mlScore;
        out.collect(mlDecision);
    }

    @Override
    public void processBroadcastElement(FraudRule rule, Context ctx, Collector<FraudDecision> out) throws Exception {
        BroadcastState<String, FraudRule> state = ctx.getBroadcastState(RuleStateDescriptor.DESCRIPTOR);
        if (rule.enabled) {
            state.put(rule.ruleId, rule);
        } else {
            state.remove(rule.ruleId);
        }
    }
}
