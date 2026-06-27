package com.frauddetection.cep.patterns;

import com.frauddetection.common.model.Transaction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * BLOCK: transactions from 2+ distinct countries for the same account within a
 * 1-hour window.
 */
public class LocationJumpPattern implements CepPattern {

    private final Duration window;
    private final ListState<Tuple2<Long, String>> recentLocations;

    public LocationJumpPattern(RuntimeContext ctx, ParameterTool params) {
        this.window = Duration.ofSeconds(params.getLong("cep.location-jump.window.seconds", 3600));
        Duration stateTtl = Duration.ofMinutes(params.getLong("cep.location-jump.state.ttl.minutes", 5));

        ListStateDescriptor<Tuple2<Long, String>> descriptor = new ListStateDescriptor<>(
                "location-jump-recent",
                TypeInformation.of(new TypeHint<Tuple2<Long, String>>() {}));
        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(stateTtl).build());
        this.recentLocations = ctx.getListState(descriptor);
    }

    @Override
    public String getName() {
        return "LOCATION_JUMP";
    }

    @Override
    public boolean matches(Transaction tx) throws Exception {
        long eventTimeMs = Instant.parse(tx.eventTime).toEpochMilli();

        List<Tuple2<Long, String>> recent = new ArrayList<>();
        Iterable<Tuple2<Long, String>> stored = recentLocations.get();
        if (stored != null) {
            for (Tuple2<Long, String> entry : stored) {
                if (eventTimeMs - entry.f0 <= window.toMillis()) {
                    recent.add(entry);
                }
            }
        }
        recent.add(Tuple2.of(eventTimeMs, tx.locationName));
        recentLocations.update(recent);

        Set<String> distinctLocations = new HashSet<>();
        for (Tuple2<Long, String> entry : recent) {
            distinctLocations.add(entry.f1);
        }
        return distinctLocations.size() >= 2;
    }
}
