package com.frauddetection.rules.rules;

import com.frauddetection.common.model.DecisionStatus;
import com.frauddetection.common.model.Transaction;

import java.io.Serializable;
import java.util.Optional;

public interface Rule extends Serializable {

    String getName();

    Optional<DecisionStatus> evaluate(Transaction tx);
}
