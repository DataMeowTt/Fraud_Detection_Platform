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


if __name__ == "__main__":
    print_avg_latency()
    
    print("\n")
    
    print_latency_percentiles()

    print("\n")
    
    print_top_slowest()
