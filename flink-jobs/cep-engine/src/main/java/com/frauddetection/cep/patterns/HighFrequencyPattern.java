package com.frauddetection.cep.patterns;

import com.frauddetection.common.model.Transaction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;

import java.time.Duration;

/**
 * BLOCK: 3+ transactions with amount above 50,000,000 for the same account
 * within a 120s window.
 */
public class HighFrequencyPattern extends WindowedCountPattern {

    private final long amountThreshold;

    public HighFrequencyPattern(RuntimeContext ctx, ParameterTool params) {
        super(ctx, "high-frequency-timestamps",
                params.getInt("cep.high-frequency.threshold", 3),
                Duration.ofSeconds(params.getLong("cep.window.seconds", 120)),
                Duration.ofMinutes(params.getLong("cep.state.ttl.minutes", 30)));
        this.amountThreshold = params.getLong("cep.high-frequency.amount-min", 50_000_000L);
    }

    @Override
    public String getName() {
        return "HIGH_FREQUENCY";
    }

    @Override
    protected boolean isQualifying(Transaction tx) {
        return tx.amount > amountThreshold;
    }
}
