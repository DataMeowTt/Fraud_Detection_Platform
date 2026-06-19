package com.frauddetection.rules.state;

import com.frauddetection.common.model.FraudRule;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class RuleStateDescriptor {

    public static final MapStateDescriptor<String, FraudRule> DESCRIPTOR =
            new MapStateDescriptor<>("rules-state", Types.STRING, TypeInformation.of(FraudRule.class));
}
