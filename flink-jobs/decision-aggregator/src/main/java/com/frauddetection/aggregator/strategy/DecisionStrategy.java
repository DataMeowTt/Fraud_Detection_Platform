package com.frauddetection.aggregator.strategy;

import com.frauddetection.common.model.DecisionStatus;
import com.frauddetection.common.model.FraudDecision;
import org.apache.flink.api.common.functions.MapFunction;

public class DecisionStrategy implements MapFunction<FraudDecision, FraudDecision> {

    @Override
    public FraudDecision map(FraudDecision decision) {
        // Priority 1: Rules Engine — hard BLOCK, highest confidence
        if (decision.ruleName != null && decision.status == DecisionStatus.BLOCK) {
            return decision;
        }

        // TODO Priority 2: CEP Engine — pattern-based BLOCK or ALERT
        // if (decision.cepPattern != null) {
        //     decision.status = isSevereCepPattern(decision.cepPattern)
        //         ? DecisionStatus.BLOCK
        //         : DecisionStatus.ALERT;
        //     return decision;
        // }

        // TODO Priority 3: ML Engine — probability-based ALERT
        // if (decision.mlScore != null && decision.mlScore >= ML_ALERT_THRESHOLD) {
        //     decision.status = DecisionStatus.ALERT;
        //     return decision;
        // }

        decision.status = DecisionStatus.APPROVED;
        return decision;
    }
}
