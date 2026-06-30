import os
from datetime import datetime

import clickhouse_connect

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")

def get_client():
    return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=8123)


def print_avg_latency():
    client = get_client()
    try:
        result = client.query("""
            SELECT avg(dateDiff('millisecond', produced_at, decided_at)) AS avg_latency_ms
            FROM fraud_detection.transactions
            WHERE account_id != 'FRD_RULE_UPDATE_SC6'
              AND decided_at >= (SELECT min(decided_at) + INTERVAL 10 SECOND FROM fraud_detection.transactions WHERE account_id != 'FRD_RULE_UPDATE_SC6')
        """)
        avg_ms = result.result_rows[0][0]
        print(f"[Latency] avg(decided_at - produced_at) = {avg_ms:.2f} ms")
    finally:
        client.close()


def print_top_slowest():
    client = get_client()
    try:
        result = client.query("""
            SELECT
                account_id,
                decision,
                produced_at,
                decided_at,
                dateDiff('millisecond', produced_at, decided_at) AS latency_ms
            FROM fraud_detection.transactions
            WHERE account_id != 'FRD_RULE_UPDATE_SC6'
              AND decided_at >= (SELECT min(decided_at) + INTERVAL 10 SECOND FROM fraud_detection.transactions WHERE account_id != 'FRD_RULE_UPDATE_SC6')
            ORDER BY latency_ms DESC
            LIMIT 20
        """)
        col_widths = [10, 10, 26, 26, 10]
        cols = ["account_id", "decision", "produced_at", "decided_at", "latency_ms"]
        header = " | ".join(f"{c:>{w}}" for c, w in zip(cols, col_widths))
        print("\n[Top slowest decisions]")
        print(header)
        print("-" * len(header))
        for row in result.result_rows:
            print(" | ".join(f"{str(v):>{w}}" for v, w in zip(row, col_widths)))
    finally:
        client.close()


def print_latency_percentiles():
    client = get_client()
    try:
        result = client.query("""
            SELECT
                quantile(0.50)(dateDiff('millisecond', produced_at, decided_at)) AS p50_ms,
                quantile(0.90)(dateDiff('millisecond', produced_at, decided_at)) AS p90_ms,
                quantile(0.95)(dateDiff('millisecond', produced_at, decided_at)) AS p95_ms,
                quantile(0.99)(dateDiff('millisecond', produced_at, decided_at)) AS p99_ms,
                max(dateDiff('millisecond', produced_at, decided_at))            AS max_ms
            FROM fraud_detection.transactions
            WHERE account_id != 'FRD_RULE_UPDATE_SC6'
              AND decided_at >= (SELECT min(decided_at) + INTERVAL 10 SECOND FROM fraud_detection.transactions WHERE account_id != 'FRD_RULE_UPDATE_SC6')
        """)
        p50, p90, p95, p99, max_ms = result.result_rows[0]
        print(f"[Latency] p50 = {p50:.0f} ms")
        print(f"[Latency] p90 = {p90:.0f} ms")
        print(f"[Latency] p95 = {p95:.0f} ms")
        print(f"[Latency] p99 = {p99:.0f} ms")
        print(f"[Latency] max = {max_ms:.0f} ms")
    finally:
        client.close()


def print_rule_update_latency(client) -> None:
    result = client.query(
        """
        SELECT decided_at
        FROM fraud_detection.transactions
        WHERE account_id = 'FRD_RULE_UPDATE_SC6'
          AND decision = 'BLOCK'
        ORDER BY decided_at ASC
        LIMIT 1
        """
    )
    if not result.result_rows:
        print("[Latency] No BLOCK decision found for FRD_RULE_UPDATE_SC6")
        return

    decided_at = result.result_rows[0][0]

    txt_path = os.path.join(os.path.dirname(__file__), "..", "rule_update_time.txt")
    with open(txt_path) as f:
        rule_time_str = f.read().strip()
    rule_time = datetime.strptime(rule_time_str, "%Y-%m-%d %H:%M:%S.%f")

    if isinstance(decided_at, str):
        decided_at = datetime.strptime(decided_at[:26], "%Y-%m-%d %H:%M:%S.%f")

    latency_s = (decided_at - rule_time).total_seconds()
    print(f"[Latency] Rule produced_at : {rule_time_str}")
    print(f"[Latency] First BLOCK at   : {decided_at}")
    print(f"[Latency] Rule → BLOCK     : {latency_s:.3f} s")


if __name__ == "__main__":
    print_avg_latency()
    
    print("\n")
    
    print_latency_percentiles()

    print("\n")
    
    print_top_slowest()
    
    print("\n")
    
    print_rule_update_latency(get_client())
