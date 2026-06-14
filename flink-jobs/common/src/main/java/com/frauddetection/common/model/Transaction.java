package com.frauddetection.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction implements Serializable {

    @JsonProperty("transaction_id")
    public String transactionId;

    @JsonProperty("produced_at")
    public String producedAt;

    @JsonProperty("event_time")
    public String eventTime;

    @JsonProperty("account_id")
    public String accountId;

    @JsonProperty("card_id")
    public String cardId;

    @JsonProperty("amount")
    public long amount;

    @JsonProperty("channel")
    public String channel;

    @JsonProperty("location_name")
    public String locationName;

    @JsonProperty("status")
    public String status;

    @JsonProperty("is_fraud")
    public String isFraud;

    @JsonProperty("fraud_pattern")
    public String fraudPattern;

    public Transaction() {}
}
