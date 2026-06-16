package com.frauddetection.cep.processor;

import com.frauddetection.common.model.DecisionStatus;
import com.frauddetection.common.model.Transaction;

import java.util.Optional;

/**
 * TODO: implement Flink CEP pattern matching (FailedPinPattern, MultiCountryLoginPattern,
 * PasswordChangeTransferPattern, ...). For now this is a stub that never fires, so every
 * transaction falls through to the ML stage.
 */
public class CepEvaluator {

    public Optional<CepResult> evaluate(Transaction tx) {
        return Optional.empty();
    }

    public record CepResult(DecisionStatus status, String patternName) {}
}
