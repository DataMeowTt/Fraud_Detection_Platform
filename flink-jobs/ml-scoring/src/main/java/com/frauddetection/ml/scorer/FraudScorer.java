package com.frauddetection.ml.scorer;

import com.frauddetection.common.model.Transaction;

import java.util.Optional;

/**
 * TODO: load the trained XGBoost model via ModelLoader and score using features from
 * FeatureExtractor. For now this is a stub that never scores, so every transaction
 * defaults to APPROVED when it reaches this stage.
 */
public class FraudScorer {

    public Optional<Float> score(Transaction tx) {
        return Optional.empty();
    }
}
