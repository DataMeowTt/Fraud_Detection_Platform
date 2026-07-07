package com.frauddetection.common.util;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Splits a stream of Either<L, R> into a main output of L and a side output of R.
 * Used to route DLQ records (R) away from the main pipeline (L) after deserialization.
 */
public class EitherSplitFunction<L, R> extends ProcessFunction<Either<L, R>, L> {

    private final OutputTag<R> sideOutputTag;

    public EitherSplitFunction(OutputTag<R> sideOutputTag) {
        this.sideOutputTag = sideOutputTag;
    }

    @Override
    public void processElement(Either<L, R> value, Context ctx, Collector<L> out) {
        if (value.isLeft()) {
            out.collect(value.left());
        } else {
            ctx.output(sideOutputTag, value.right());
        }
    }
}
