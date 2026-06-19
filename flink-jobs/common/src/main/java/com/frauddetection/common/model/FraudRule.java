package com.frauddetection.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FraudRule implements Serializable {

    @JsonProperty("rule_id")
    public String ruleId;

    @JsonProperty("field")
    public String field;       // "amount", "channel", "status"

    @JsonProperty("operator")
    public String operator;    // "GT", "GTE", "LT", "LTE", "EQ", "NEQ"

    @JsonProperty("threshold")
    public long threshold;     // dùng cho numeric fields

    @JsonProperty("value")
    public String value;       // dùng cho string fields

    @JsonProperty("action")
    public String action;      // "BLOCK", "ALERT"

    @JsonProperty("enabled")
    public boolean enabled;

    public FraudRule() {}
}
