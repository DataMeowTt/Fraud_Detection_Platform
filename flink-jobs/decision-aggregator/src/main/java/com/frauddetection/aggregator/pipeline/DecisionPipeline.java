package com.frauddetection.aggregator.pipeline;

import com.frauddetection.cep.processor.CepEvaluator;
import com.frauddetection.common.model.FraudDecision;
import com.frauddetection.common.model.FraudRule;
import com.frauddetection.common.model.Transaction;
import com.frauddetection.rules.processor.RuleEvaluator;
import com.frauddetection.rules.state.RuleStateDescriptor;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Optional;

/**
 *   transaction -> Rules Engine -> BLOCK/ALERT? -> output
 *                  Rules PASS   -> CEP Engine   -> BLOCK/ALERT? -> output
 *                  CEP PASS                     -> APPROVED
 */
public class DecisionPipeline
        extends KeyedBroadcastProcessFunction<String, Transaction, FraudRule, FraudDecision> {

    private RuleEvaluator ruleEvaluator;
    private CepEvaluator  cepEvaluator;

    @Override
    public void open(Configuration parameters) throws Exception {
        ruleEvaluator = new RuleEvaluator();
        cepEvaluator  = new CepEvaluator(getRuntimeContext());
    }

    @Override
    public void processElement(Transaction tx, ReadOnlyContext ctx, Collector<FraudDecision> out) throws Exception {

        ReadOnlyBroadcastState<String, FraudRule> rules = ctx.getBroadcastState(RuleStateDescriptor.DESCRIPTOR);

        Optional<FraudDecision> ruleDecision = ruleEvaluator.evaluate(tx, rules.immutableEntries());
        if (ruleDecision.isPresent()) {
            out.collect(ruleDecision.get());
            return;
        }

        Optional<CepEvaluator.CepResult> cepResult = cepEvaluator.evaluate(tx);
        if (cepResult.isPresent()) {
            FraudDecision decision = new FraudDecision(tx, cepResult.get().status(), null);
            decision.cepPattern = cepResult.get().patternName();
            out.collect(decision);
            return;
        }

        out.collect(new FraudDecision(tx, com.frauddetection.common.model.DecisionStatus.APPROVED, null));
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
