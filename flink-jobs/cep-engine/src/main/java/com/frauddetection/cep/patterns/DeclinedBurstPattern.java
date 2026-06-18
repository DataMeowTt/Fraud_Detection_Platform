package com.frauddetection.cep.patterns;

import com.frauddetection.common.model.Transaction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;

import java.time.Duration;

/**
 * ALERT: 3+ Declined transactions for the same account within a 120s window.
 */
public class DeclinedBurstPattern extends WindowedCountPattern {

    public DeclinedBurstPattern(RuntimeContext ctx, ParameterTool params) {
        super(ctx, "declined-burst-timestamps",
                params.getInt("cep.declined-burst.threshold", 3),
                Duration.ofSeconds(params.getLong("cep.window.seconds", 120)),
                Duration.ofMinutes(params.getLong("cep.state.ttl.minutes", 30)));
    }

    @Override
    public String getName() {
        return "DECLINED_BURST";
    }

    @Override
    protected boolean isQualifying(Transaction tx) {
        return "Declined".equalsIgnoreCase(tx.status);
    }
}
