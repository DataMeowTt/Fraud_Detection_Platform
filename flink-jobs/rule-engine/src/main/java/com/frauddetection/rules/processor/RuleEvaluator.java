package com.frauddetection.rules.processor;

import com.frauddetection.common.model.DecisionStatus;
import com.frauddetection.common.model.FraudDecision;
import com.frauddetection.common.model.FraudRule;
import com.frauddetection.common.model.Transaction;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;

public class RuleEvaluator implements Serializable {

    public Optional<FraudDecision> evaluate(Transaction tx, Iterable<Map.Entry<String, FraudRule>> rules) {
        for (Map.Entry<String, FraudRule> entry : rules) {
            FraudRule rule = entry.getValue();
            if (rule.enabled && matches(rule, tx)) {
                DecisionStatus status = "BLOCK".equals(rule.action) ? DecisionStatus.BLOCK : DecisionStatus.ALERT;
                return Optional.of(new FraudDecision(tx, status, rule.ruleId));
            }
        }
        return Optional.empty();
    }

    private boolean matches(FraudRule rule, Transaction tx) {
        switch (rule.field) {
            case "amount":     return compareNumeric(tx.amount,   rule.operator, rule.threshold);
            case "account_id": return compareString(tx.accountId, rule.operator, rule.value);
            default:           return false;
        }
    }

    private boolean compareNumeric(long actual, String op, long threshold) {
        switch (op) {
            case "GT":  return actual >  threshold;
            case "GTE": return actual >= threshold;
            case "LT":  return actual <  threshold;
            case "LTE": return actual <= threshold;
            case "EQ":  return actual == threshold;
            case "NEQ": return actual != threshold;
            default:    return false;
        }
    }

    private boolean compareString(String actual, String op, String value) {
        if (actual == null || value == null) return false;
        switch (op) {
            case "EQ":  return actual.equals(value);
            case "NEQ": return !actual.equals(value);
            default:    return false;
        }
    }
}
