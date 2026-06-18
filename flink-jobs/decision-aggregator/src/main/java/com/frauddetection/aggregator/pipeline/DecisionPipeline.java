package com.frauddetection.aggregator.pipeline;

import com.frauddetection.cep.processor.CepEvaluator;
import com.frauddetection.common.model.DecisionStatus;
import com.frauddetection.common.model.FraudDecision;
import com.frauddetection.common.model.Transaction;
// import com.frauddetection.ml.scorer.FraudScorer;
import com.frauddetection.rules.processor.RuleEvaluator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.Optional;

/**
 *   transaction -> Rules Engine -> BLOCK?        -> output
 *                  Rules PASS   -> CEP Engine    -> BLOCK/ALERT?        -> output   
 *                  CEP PASS     -> ML Engine     -> score >= threshold? -> output 
 *                  else                          -> APPROVED
 */
public class DecisionPipeline extends RichMapFunction<Transaction, FraudDecision> {

    // private static final float ML_ALERT_THRESHOLD = 0.8f; // placeholder until ML stage is implemented

    private RuleEvaluator ruleEvaluator;
    private CepEvaluator  cepEvaluator;
    // private FraudScorer   fraudScorer;

    @Override
    public void open(Configuration parameters) throws Exception {
        ruleEvaluator = new RuleEvaluator();
        ruleEvaluator.open(parameters);
        cepEvaluator = new CepEvaluator(getRuntimeContext());
        // fraudScorer  = new FraudScorer();
    }

    @Override
    public FraudDecision map(Transaction tx) throws Exception {

        FraudDecision ruleDecision = ruleEvaluator.map(tx);
        if (ruleDecision.status == DecisionStatus.BLOCK) {
            return ruleDecision;
        }

        Optional<CepEvaluator.CepResult> cepResult = cepEvaluator.evaluate(tx);
        if (cepResult.isPresent()) {
            FraudDecision decision = new FraudDecision(tx, cepResult.get().status(), null);
            decision.cepPattern = cepResult.get().patternName();
            return decision;
        }

        // Optional<Float> mlScore = fraudScorer.score(tx);
        // if (mlScore.isPresent() && mlScore.get() >= ML_ALERT_THRESHOLD) {
        //     FraudDecision decision = new FraudDecision(tx, DecisionStatus.ALERT, null);
        //     decision.mlScore = mlScore.get();
        //     return decision;
        // }

        FraudDecision decision = new FraudDecision(tx, DecisionStatus.APPROVED, null);
        // mlScore.ifPresent(score -> decision.mlScore = score);
        return decision;
    }
}

// ToDo: finish ML logic later