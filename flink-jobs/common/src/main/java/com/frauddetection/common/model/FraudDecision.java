package com.frauddetection.common.model;

import java.io.Serializable;

public class FraudDecision implements Serializable {

    public String transactionId;
    public String accountId;
    public long   amount;
    public DecisionStatus status;

    public String ruleName;    // set by Rules Engine,  null if no rule hit
    public String cepPattern;  // set by CEP Engine,    null if no pattern hit  (TODO)
    public Float  mlScore;     // set by ML Engine,     null if not scored      (TODO)

    public String producedAt;
    public String decidedAt;

    public FraudDecision() {}

    public FraudDecision(Transaction tx, DecisionStatus status, String ruleName) {
        this.transactionId = tx.transactionId;
        this.accountId     = tx.accountId;
        this.amount        = tx.amount;
        this.producedAt    = tx.producedAt;
        this.status        = status;
        this.ruleName      = ruleName;
        this.decidedAt     = java.time.Instant.now().toString();
    }
}
