package com.frauddetection.rules.processor;

import com.frauddetection.common.model.DecisionStatus;
import com.frauddetection.common.model.FraudDecision;
import com.frauddetection.common.model.Transaction;
import com.frauddetection.rules.rules.AmountThresholdRule;
import com.frauddetection.rules.rules.Rule;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.util.List;
import java.util.Optional;

public class RuleEvaluator extends RichMapFunction<Transaction, FraudDecision> {

    private List<Rule> rules;

    @Override
    public void open(Configuration parameters) {
        rules = List.of(
            new AmountThresholdRule()
        );
    }

    @Override
    public FraudDecision map(Transaction tx) {
        for (Rule rule : rules) {
            Optional<DecisionStatus> result = rule.evaluate(tx);
            if (result.isPresent()) {
                return new FraudDecision(tx, result.get(), rule.getName());
            }
        }
        return new FraudDecision(tx, DecisionStatus.APPROVED, null);
    }
}
