package com.frauddetection.cep;

import com.frauddetection.cep.patterns.CepPattern;
import com.frauddetection.cep.patterns.DeclinedBurstPattern;
import com.frauddetection.cep.patterns.HighFrequencyPattern;
import com.frauddetection.cep.patterns.LocationJumpPattern;
import com.frauddetection.cep.patterns.RapidMicropaymentsPattern;
import com.frauddetection.common.model.Transaction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CepPatternTest {

    private static final ParameterTool EMPTY_PARAMS = ParameterTool.fromMap(Map.of());

    private static class PatternHarnessFunction extends KeyedProcessFunction<String, Transaction, Boolean> {
        private final BiFunction<RuntimeContext, ParameterTool, CepPattern> factory;
        private transient CepPattern pattern;

        PatternHarnessFunction(BiFunction<RuntimeContext, ParameterTool, CepPattern> factory) {
            this.factory = factory;
        }

        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) {
            pattern = factory.apply(getRuntimeContext(), EMPTY_PARAMS);
        }

        @Override
        public void processElement(Transaction tx, Context ctx, Collector<Boolean> out) throws Exception {
            out.collect(pattern.matches(tx));
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<String, Transaction, Boolean> newHarness(
            BiFunction<RuntimeContext, ParameterTool, CepPattern> factory) throws Exception {
        KeySelector<Transaction, String> keySelector = tx -> tx.accountId;
        KeyedOneInputStreamOperatorTestHarness<String, Transaction, Boolean> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(new PatternHarnessFunction(factory)),
                        keySelector,
                        BasicTypeInfo.STRING_TYPE_INFO);
        harness.open();
        return harness;
    }

    private static Transaction tx(String accountId, long amount, String status, String location, Instant eventTime) {
        Transaction t = new Transaction();
        t.transactionId = accountId + "-" + eventTime.toEpochMilli();
        t.accountId = accountId;
        t.amount = amount;
        t.status = status;
        t.locationName = location;
        t.eventTime = eventTime.toString();
        return t;
    }

    private static boolean lastResult(KeyedOneInputStreamOperatorTestHarness<String, Transaction, Boolean> harness) {
        List<Boolean> results = harness.extractOutputValues();
        return results.get(results.size() - 1);
    }

    @Test
    void highFrequency_triggersOnThirdQualifyingTransactionWithinWindow() throws Exception {
        var harness = newHarness(HighFrequencyPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        harness.processElement(tx("ACC1", 120_000_000L, "Approved", "Hanoi", base), base.toEpochMilli());
        assertFalse(lastResult(harness), "1st qualifying tx alone should not trigger");

        harness.processElement(tx("ACC1", 120_000_000L, "Approved", "Hanoi", base.plusSeconds(60)), base.plusSeconds(60).toEpochMilli());
        assertFalse(lastResult(harness), "2nd qualifying tx should not yet trigger");

        harness.processElement(tx("ACC1", 120_000_000L, "Approved", "Hanoi", base.plusSeconds(120)), base.plusSeconds(120).toEpochMilli());
        assertTrue(lastResult(harness), "3rd qualifying tx within window should trigger");
    }

    @Test
    void highFrequency_ignoresTransactionsBelowAmountThreshold() throws Exception {
        var harness = newHarness(HighFrequencyPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        for (int i = 0; i < 5; i++) {
            harness.processElement(
                    tx("ACC1", 50_000_000L, "Approved", "Hanoi", base.plusSeconds(i * 10L)),
                    base.plusSeconds(i * 10L).toEpochMilli());
            assertFalse(lastResult(harness), "amount below threshold must never count");
        }
    }

    @Test
    void highFrequency_ignoresNonApprovedStatus() throws Exception {
        var harness = newHarness(HighFrequencyPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        for (int i = 0; i < 5; i++) {
            harness.processElement(
                    tx("ACC1", 150_000_000L, "Declined", "Hanoi", base.plusSeconds(i * 10L)),
                    base.plusSeconds(i * 10L).toEpochMilli());
            assertFalse(lastResult(harness), "non-Approved status must never count");
        }
    }

    @Test
    void highFrequency_doesNotTriggerWhenSpreadOutsideWindow() throws Exception {
        var harness = newHarness(HighFrequencyPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        harness.processElement(tx("ACC1", 120_000_000L, "Approved", "Hanoi", base), base.toEpochMilli());
        harness.processElement(tx("ACC1", 120_000_000L, "Approved", "Hanoi", base.plusSeconds(1900)), base.plusSeconds(1900).toEpochMilli());
        Instant third = base.plusSeconds(3700);
        harness.processElement(tx("ACC1", 120_000_000L, "Approved", "Hanoi", third), third.toEpochMilli());

        assertFalse(lastResult(harness), "only 2 tx remain inside the rolling 3600s window");
    }

    @Test
    void highFrequency_isIsolatedPerAccount() throws Exception {
        var harness = newHarness(HighFrequencyPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        harness.processElement(tx("ACC1", 120_000_000L, "Approved", "Hanoi", base), base.toEpochMilli());
        harness.processElement(tx("ACC1", 120_000_000L, "Approved", "Hanoi", base.plusSeconds(10)), base.plusSeconds(10).toEpochMilli());
        harness.processElement(tx("ACC2", 120_000_000L, "Approved", "Hanoi", base.plusSeconds(20)), base.plusSeconds(20).toEpochMilli());

        assertFalse(lastResult(harness), "ACC2's first tx must not be affected by ACC1's history");
    }

    @Test
    void locationJump_triggersOnSecondDistinctLocationWithinWindow() throws Exception {
        var harness = newHarness(LocationJumpPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        harness.processElement(tx("ACC1", 500_000L, "Approved", "Hanoi", base), base.toEpochMilli());
        assertFalse(lastResult(harness), "single location must not trigger");

        harness.processElement(tx("ACC1", 500_000L, "Approved", "Bangkok", base.plusSeconds(30)), base.plusSeconds(30).toEpochMilli());
        assertTrue(lastResult(harness), "2nd distinct location within window must trigger");
    }

    @Test
    void locationJump_doesNotTriggerOnRepeatedSameLocation() throws Exception {
        var harness = newHarness(LocationJumpPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        for (int i = 0; i < 5; i++) {
            harness.processElement(
                    tx("ACC1", 500_000L, "Approved", "Hanoi", base.plusSeconds(i * 10L)),
                    base.plusSeconds(i * 10L).toEpochMilli());
            assertFalse(lastResult(harness), "same location repeated must never trigger");
        }
    }

    @Test
    void locationJump_doesNotTriggerWhenSpreadOutsideWindow() throws Exception {
        var harness = newHarness(LocationJumpPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        harness.processElement(tx("ACC1", 500_000L, "Approved", "Hanoi", base), base.toEpochMilli());
        Instant later = base.plusSeconds(3700);
        harness.processElement(tx("ACC1", 500_000L, "Approved", "Bangkok", later), later.toEpochMilli());

        assertFalse(lastResult(harness), "Hanoi tx fell out of the window, only 1 location remains");
    }

    @Test
    void declinedBurst_triggersOnThirdDeclineWithinWindow() throws Exception {
        var harness = newHarness(DeclinedBurstPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        harness.processElement(tx("ACC1", 50_000L, "Declined", "Hanoi", base), base.toEpochMilli());
        assertFalse(lastResult(harness));
        harness.processElement(tx("ACC1", 50_000L, "Declined", "Hanoi", base.plusSeconds(20)), base.plusSeconds(20).toEpochMilli());
        assertFalse(lastResult(harness));
        harness.processElement(tx("ACC1", 50_000L, "Declined", "Hanoi", base.plusSeconds(40)), base.plusSeconds(40).toEpochMilli());
        assertTrue(lastResult(harness));
    }

    @Test
    void declinedBurst_statusMatchIsCaseInsensitive() throws Exception {
        var harness = newHarness(DeclinedBurstPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        harness.processElement(tx("ACC1", 50_000L, "declined", "Hanoi", base), base.toEpochMilli());
        harness.processElement(tx("ACC1", 50_000L, "DECLINED", "Hanoi", base.plusSeconds(10)), base.plusSeconds(10).toEpochMilli());
        harness.processElement(tx("ACC1", 50_000L, "Declined", "Hanoi", base.plusSeconds(20)), base.plusSeconds(20).toEpochMilli());

        assertTrue(lastResult(harness));
    }

    @Test
    void declinedBurst_ignoresApprovedTransactions() throws Exception {
        var harness = newHarness(DeclinedBurstPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        for (int i = 0; i < 5; i++) {
            harness.processElement(
                    tx("ACC1", 50_000L, "Approved", "Hanoi", base.plusSeconds(i * 5L)),
                    base.plusSeconds(i * 5L).toEpochMilli());
            assertFalse(lastResult(harness));
        }
    }

    @Test
    void declinedBurst_doesNotTriggerWhenSpreadOutsideWindow() throws Exception {
        var harness = newHarness(DeclinedBurstPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        harness.processElement(tx("ACC1", 50_000L, "Declined", "Hanoi", base), base.toEpochMilli());
        harness.processElement(tx("ACC1", 50_000L, "Declined", "Hanoi", base.plusSeconds(60)), base.plusSeconds(60).toEpochMilli());
        Instant third = base.plusSeconds(150);
        harness.processElement(tx("ACC1", 50_000L, "Declined", "Hanoi", third), third.toEpochMilli());

        assertFalse(lastResult(harness), "1st decline fell outside the 120s window");
    }

    @Test
    void rapidMicropayments_triggersOnThirdQualifyingTransactionWithinWindow() throws Exception {
        var harness = newHarness(RapidMicropaymentsPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        harness.processElement(tx("ACC1", 5_000L, "Approved", "Hanoi", base), base.toEpochMilli());
        assertFalse(lastResult(harness));
        harness.processElement(tx("ACC1", 5_000L, "Approved", "Hanoi", base.plusSeconds(10)), base.plusSeconds(10).toEpochMilli());
        assertFalse(lastResult(harness));
        harness.processElement(tx("ACC1", 5_000L, "Approved", "Hanoi", base.plusSeconds(20)), base.plusSeconds(20).toEpochMilli());
        assertTrue(lastResult(harness));
    }

    @Test
    void rapidMicropayments_respectsAmountBoundaries() throws Exception {
        var harness = newHarness(RapidMicropaymentsPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        harness.processElement(tx("ACC1", 2_000L, "Approved", "Hanoi", base), base.toEpochMilli());
        harness.processElement(tx("ACC1", 10_000L, "Approved", "Hanoi", base.plusSeconds(5)), base.plusSeconds(5).toEpochMilli());
        harness.processElement(tx("ACC1", 6_000L, "Approved", "Hanoi", base.plusSeconds(10)), base.plusSeconds(10).toEpochMilli());
        assertTrue(lastResult(harness), "boundary amounts (2_000 and 10_000) should qualify");
    }

    @Test
    void rapidMicropayments_ignoresAmountsOutsideRange() throws Exception {
        var harness = newHarness(RapidMicropaymentsPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        harness.processElement(tx("ACC1", 1_999L, "Approved", "Hanoi", base), base.toEpochMilli());
        harness.processElement(tx("ACC1", 10_001L, "Approved", "Hanoi", base.plusSeconds(5)), base.plusSeconds(5).toEpochMilli());
        harness.processElement(tx("ACC1", 1_000_000L, "Approved", "Hanoi", base.plusSeconds(10)), base.plusSeconds(10).toEpochMilli());

        assertFalse(lastResult(harness), "amounts outside [2_000, 10_000] must never count");
    }

    @Test
    void rapidMicropayments_doesNotTriggerWhenSpreadOutsideWindow() throws Exception {
        var harness = newHarness(RapidMicropaymentsPattern::new);
        Instant base = Instant.parse("2026-01-01T00:00:00Z");

        harness.processElement(tx("ACC1", 5_000L, "Approved", "Hanoi", base), base.toEpochMilli());
        harness.processElement(tx("ACC1", 5_000L, "Approved", "Hanoi", base.plusSeconds(60)), base.plusSeconds(60).toEpochMilli());
        Instant third = base.plusSeconds(150);
        harness.processElement(tx("ACC1", 5_000L, "Approved", "Hanoi", third), third.toEpochMilli());

        assertFalse(lastResult(harness), "1st tx fell outside the 120s window");
    }
}
