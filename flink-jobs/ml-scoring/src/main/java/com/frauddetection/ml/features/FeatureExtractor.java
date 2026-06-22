package com.frauddetection.ml.features;

import com.frauddetection.common.model.Transaction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class FeatureExtractor implements Serializable {

    private static final Set<String> DOMESTIC_LOCS = Set.of(
            "Hanoi", "HCM City", "Da Nang", "Can Tho", "Hai Phong"
    );
    private transient ListState<Tuple2<Long, Long>> txHistory;
    private transient ValueState<String> homeLocation;

    public void open(RuntimeContext ctx) throws Exception {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofHours(24))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ListStateDescriptor<Tuple2<Long, Long>> histDesc = new ListStateDescriptor<>(
                "ml.txHistory",
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})
        );
        histDesc.enableTimeToLive(ttl);
        txHistory = ctx.getListState(histDesc);

        homeLocation = ctx.getState(new ValueStateDescriptor<>("ml.homeLocation", String.class));
    }

    public float[] extract(Transaction tx) throws Exception {
        long currentTs = Instant.parse(tx.eventTime).toEpochMilli();

        float amount     = (float) tx.amount;
        float hourOfDay  = (float) Instant.ofEpochMilli(currentTs).atZone(ZoneOffset.UTC).getHour();
        float isDomestic = DOMESTIC_LOCS.contains(tx.locationName) ? 1.0f : 0.0f;

        String home = homeLocation.value();
        float isHome = (home == null || home.equals(tx.locationName)) ? 1.0f : 0.0f;

        List<Tuple2<Long, Long>> history = new ArrayList<>();
        Iterable<Tuple2<Long, Long>> stored = txHistory.get();
        if (stored != null) {
            for (Tuple2<Long, Long> e : stored) history.add(e);
        }

        long cutoff24h = currentTs - 24L * 3_600_000;
        long cutoff1h  = currentTs -       3_600_000;

        double sumAmounts = 0;
        int    count24h   = 0;
        int    count1h    = 0;
        long   lastTs     = 0;

        for (Tuple2<Long, Long> e : history) {
            if (e.f0 >= cutoff24h) { sumAmounts += e.f1; count24h++; }
            if (e.f0 >= cutoff1h)    count1h++;
            if (e.f0 > lastTs)       lastTs = e.f0;
        }

        float avg24h    = count24h > 0 ? (float) (sumAmounts / count24h) : amount;
        float ratio     = avg24h   > 0 ? amount / avg24h : 1.0f;
        float timeSince = lastTs   > 0 ? (float) ((currentTs - lastTs) / 1000.0) : 0.0f;

        return new float[]{amount, hourOfDay, isHome, isDomestic, avg24h, ratio, (float) count1h, timeSince};
    }

    public void update(Transaction tx) throws Exception {
        long currentTs = Instant.parse(tx.eventTime).toEpochMilli();
        txHistory.add(Tuple2.of(currentTs, tx.amount));
        if (homeLocation.value() == null) {
            homeLocation.update(tx.locationName);
        }
    }
}
