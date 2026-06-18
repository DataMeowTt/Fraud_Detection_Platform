package com.frauddetection.cep.processor;

import com.frauddetection.cep.patterns.CepPattern;
import com.frauddetection.cep.patterns.DeclinedBurstPattern;
import com.frauddetection.cep.patterns.HighFrequencyPattern;
import com.frauddetection.cep.patterns.LocationJumpPattern;
import com.frauddetection.cep.patterns.RapidMicropaymentsPattern;
import com.frauddetection.common.model.DecisionStatus;
import com.frauddetection.common.model.Transaction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;


public class CepEvaluator {

    private final List<CepPattern> blockPatterns;
    private final List<CepPattern> alertPatterns;

    public CepEvaluator(RuntimeContext ctx) {
        ParameterTool params = loadParams();
        blockPatterns = List.of(
                new LocationJumpPattern(ctx, params),
                new HighFrequencyPattern(ctx, params)
        );
        alertPatterns = List.of(
                new DeclinedBurstPattern(ctx, params),
                new RapidMicropaymentsPattern(ctx, params)
        );
    }

    public Optional<CepResult> evaluate(Transaction tx) throws Exception {
        for (CepPattern pattern : blockPatterns) {
            if (pattern.matches(tx)) {
                return Optional.of(new CepResult(DecisionStatus.BLOCK, pattern.getName()));
            }
        }
        for (CepPattern pattern : alertPatterns) {
            if (pattern.matches(tx)) {
                return Optional.of(new CepResult(DecisionStatus.ALERT, pattern.getName()));
            }
        }
        return Optional.empty();
    }

    public record CepResult(DecisionStatus status, String patternName) {}

    private static ParameterTool loadParams() {
        try (InputStream is = CepEvaluator.class.getResourceAsStream("/cep-engine.properties")) {
            return is != null ? ParameterTool.fromPropertiesFile(is) : ParameterTool.fromMap(Map.of());
        } catch (IOException e) {
            return ParameterTool.fromMap(Map.of());
        }
    }
}
