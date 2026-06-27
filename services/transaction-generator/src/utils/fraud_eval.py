import os

import clickhouse_connect

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")


def get_client():
    return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=8123)


def print_fraud_detection_eval(client) -> None:
    result = client.query("""
        SELECT
            countIf(g.is_fraud = true AND t.decision = 'APPROVED')              AS false_negative,
            countIf(g.is_fraud = true AND t.decision IN ('BLOCK', 'ALERT'))     AS true_positive,
            countIf(g.is_fraud = true)                                          AS total_fraud
        FROM fraud_detection.ground_true_transactions AS g
        JOIN fraud_detection.transactions AS t
          ON g.transaction_id = t.transaction_id
        WHERE g.account_id != 'FRD_RULE_UPDATE_SC6'
    """)

    false_negative, true_positive, total_fraud = result.result_rows[0]
    detection_rate = true_positive / total_fraud if total_fraud > 0 else 0.0

    print(f"[Eval] Fraud bị bỏ sót   (is_fraud=True & APPROVED)         : {false_negative:>6,}")
    print(f"[Eval] Fraud bị bắt đúng (is_fraud=True & BLOCK/ALERT)      : {true_positive:>6,}")
    print(f"[Eval] Tổng fraud trong ground truth                        : {total_fraud:>6,}")
    print(f"[Eval] Detection rate (bắt được / tổng fraud)               : {detection_rate:.4f}  ({detection_rate*100:.2f}%)")


if __name__ == "__main__":
    client = get_client()
    try:
        print_fraud_detection_eval(client)
    finally:
        client.close()
