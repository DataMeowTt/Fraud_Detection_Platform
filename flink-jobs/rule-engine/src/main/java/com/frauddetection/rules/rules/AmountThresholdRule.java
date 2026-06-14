package com.frauddetection.rules.rules;

import com.frauddetection.common.model.DecisionStatus;
import com.frauddetection.common.model.Transaction;

import java.util.Optional;

public class AmountThresholdRule implements Rule {

    private static final long BLOCK_THRESHOLD = 1_000_000_000L;

    @Override
    public String getName() {
        return "AMOUNT_THRESHOLD";
    }

    @Override
    public Optional<DecisionStatus> evaluate(Transaction tx) {
        if (tx.amount > BLOCK_THRESHOLD) {
            return Optional.of(DecisionStatus.BLOCK);
        }
        return Optional.empty();
    }
}
