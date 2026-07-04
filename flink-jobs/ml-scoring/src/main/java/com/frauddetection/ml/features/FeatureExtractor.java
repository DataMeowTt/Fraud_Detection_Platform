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
import org.apache.flink.api.java.tuple.Tuple4;

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
    private transient ListState<Tuple4<String, Long, Long, Boolean>> txHistory;
    private transient ValueState<String> homeLocation;
    private transient Transaction cachedForTx;
    private transient List<Tuple4<String, Long, Long, Boolean>> cachedHistory;

    public void open(RuntimeContext ctx) throws Exception {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofHours(24))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ListStateDescriptor<Tuple4<String, Long, Long, Boolean>> histDesc = new ListStateDescriptor<>(
                "ml.txHistory",
                TypeInformation.of(new TypeHint<Tuple4<String, Long, Long, Boolean>>() {})
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
        float isHome = (home == null) ? isDomestic : (home.equals(tx.locationName) ? 1.0f : 0.0f);

        List<Tuple4<String, Long, Long, Boolean>> history = new ArrayList<>();
        Iterable<Tuple4<String, Long, Long, Boolean>> stored = txHistory.get();
        if (stored != null) {
            for (Tuple4<String, Long, Long, Boolean> e : stored) history.add(e);
        }
        cachedForTx   = tx;
        cachedHistory = history;

        long cutoff24h = currentTs - 24L * 3_600_000;
        long cutoff3h  = currentTs -  3L * 3_600_000;
        long cutoff1h  = currentTs -       3_600_000;

        double sumAmounts = 0;
        int    count24h   = 0;
        int    count3h    = 0;
        int    count1h    = 0;
        long   lastTs     = 0;

        for (Tuple4<String, Long, Long, Boolean> e : history) {
            if (e.f1 >= cutoff24h && e.f3) { sumAmounts += e.f2; count24h++; }
            if (e.f1 >= cutoff3h)    count3h++;
            if (e.f1 >= cutoff1h)    count1h++;
            if (e.f1 > lastTs)       lastTs = e.f1;
        }

        float avg24h         = count24h > 0 ? (float) (sumAmounts / count24h) : amount;
        float logAmountRatio = avg24h   > 0 ? (float) Math.log1p(amount / avg24h) : (float) Math.log1p(1.0);
        float timeSince      = lastTs   > 0 ? (float) ((currentTs - lastTs) / 1000.0) : 0.0f;

        return new float[]{amount, hourOfDay, isHome, isDomestic, avg24h, logAmountRatio, (float) count1h, (float) count3h, timeSince};
    }

    public void recordArrival(Transaction tx) throws Exception {
        long currentTs = Instant.parse(tx.eventTime).toEpochMilli();
        long cutoff24h = currentTs - 24L * 3_600_000;

        List<Tuple4<String, Long, Long, Boolean>> stored;
        if (cachedForTx == tx) {
            stored = cachedHistory;
        } else {
            stored = new ArrayList<>();
            Iterable<Tuple4<String, Long, Long, Boolean>> fetched = txHistory.get();
            if (fetched != null) {
                for (Tuple4<String, Long, Long, Boolean> e : fetched) stored.add(e);
            }
        }
        cachedForTx   = null;
        cachedHistory = null;

        List<Tuple4<String, Long, Long, Boolean>> kept = new ArrayList<>();
        for (Tuple4<String, Long, Long, Boolean> e : stored) {
            if (e.f1 >= cutoff24h) kept.add(e);
        }
        kept.add(Tuple4.of(tx.transactionId, currentTs, tx.amount, true));
        txHistory.update(kept);

        if (homeLocation.value() == null) {
            homeLocation.update(tx.locationName);
        }
    }

    public void finalizeAvgFlag(Transaction tx, boolean includeInAvg) throws Exception {
        if (includeInAvg) {
            return;
        }

        List<Tuple4<String, Long, Long, Boolean>> stored = new ArrayList<>();
        Iterable<Tuple4<String, Long, Long, Boolean>> fetched = txHistory.get();
        if (fetched != null) {
            for (Tuple4<String, Long, Long, Boolean> e : fetched) stored.add(e);
        }

        for (int i = 0; i < stored.size(); i++) {
            Tuple4<String, Long, Long, Boolean> e = stored.get(i);
            if (e.f0.equals(tx.transactionId)) {
                stored.set(i, Tuple4.of(e.f0, e.f1, e.f2, false));
                txHistory.update(stored);
                return;
            }
        }
    }

    public void update(Transaction tx, boolean includeInAvg) throws Exception {
        recordArrival(tx);
        finalizeAvgFlag(tx, includeInAvg);
    }
}
