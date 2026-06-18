package com.frauddetection.cep.patterns;

import com.frauddetection.common.model.Transaction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;

import java.time.Duration;

/**
 * ALERT: 3+ small transactions (amount in [2_000, 10_000]) for the same account
 * within a 120s window.
 */
public class RapidMicropaymentsPattern extends WindowedCountPattern {

    private final long minAmount;
    private final long maxAmount;

    public RapidMicropaymentsPattern(RuntimeContext ctx, ParameterTool params) {
        super(ctx, "rapid-micropayments-timestamps",
                params.getInt("cep.rapid-micropayments.threshold", 3),
                Duration.ofSeconds(params.getLong("cep.window.seconds", 120)),
                Duration.ofMinutes(params.getLong("cep.state.ttl.minutes", 30)));
        this.minAmount = params.getLong("cep.rapid-micropayments.amount-min", 2_000L);
        this.maxAmount = params.getLong("cep.rapid-micropayments.amount-max", 10_000L);
    }

    @Override
    public String getName() {
        return "RAPID_MICROPAYMENTS";
    }

    @Override
    protected boolean isQualifying(Transaction tx) {
        return tx.amount >= minAmount && tx.amount <= maxAmount;
    }
}
