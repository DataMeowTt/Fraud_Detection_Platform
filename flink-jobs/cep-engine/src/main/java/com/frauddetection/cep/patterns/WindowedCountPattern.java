package com.frauddetection.cep.patterns;

import com.frauddetection.common.model.Transaction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

abstract class WindowedCountPattern implements CepPattern {

    private final int threshold;
    private final Duration window;
    private final ListState<Long> timestamps;

    protected WindowedCountPattern(RuntimeContext ctx, String stateName, int threshold, Duration window, Duration stateTtl) {
        this.threshold = threshold;
        this.window = window;
        ListStateDescriptor<Long> descriptor = new ListStateDescriptor<>(stateName, Long.class);
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(stateTtl).build());
        this.timestamps = ctx.getListState(descriptor);
    }

    protected abstract boolean isQualifying(Transaction tx);

    @Override
    public boolean matches(Transaction tx) throws Exception {
        if (!isQualifying(tx)) {
            return false;
        }

        long eventTimeMs = Instant.parse(tx.eventTime).toEpochMilli();

        List<Long> recent = new ArrayList<>();
        Iterable<Long> stored = timestamps.get();
        if (stored != null) {
            for (Long t : stored) {
                if (eventTimeMs - t <= window.toMillis()) {
                    recent.add(t);
                }
            }
        }
        recent.add(eventTimeMs);
        timestamps.update(recent);

        return recent.size() >= threshold;
    }
}
