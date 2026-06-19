import clickhouse_connect
import os

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")

def get_client():
    return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=8123)


def print_avg_latency():
    client = get_client()
    try:
        result = client.query("""
            SELECT avg(dateDiff('millisecond', produced_at, decided_at)) AS avg_latency_ms
            FROM fraud_detection.transactions
        """)
        avg_ms = result.result_rows[0][0]
        print(f"[Latency] avg(decided_at - produced_at) = {avg_ms:.2f} ms")
    finally:
        client.close()


if __name__ == "__main__":
    print_avg_latency()
