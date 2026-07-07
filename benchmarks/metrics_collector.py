import argparse
import csv
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime

import clickhouse_connect

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
FLINK_URL       = os.getenv("FLINK_URL", "http://localhost:8081")
EXCLUDED_ACCOUNT_ID = "1"  # RuleEngineTest's synthetic test account
LEVELS_INPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "performance", "load_test_levels.json")
CSV_OUTPUT_PATH   = os.path.join(os.path.dirname(__file__), "..", "performance", "stress_test_results.csv")

def get_client():
    return clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=8123)

def query_level_latency(client, level: dict):
    result = client.query(
        """
        SELECT
            count()                                                          AS cnt,
            avg(dateDiff('millisecond', produced_at, decided_at))            AS avg_ms,
            quantile(0.50)(dateDiff('millisecond', produced_at, decided_at)) AS p50_ms,
            quantile(0.95)(dateDiff('millisecond', produced_at, decided_at)) AS p95_ms,
            quantile(0.99)(dateDiff('millisecond', produced_at, decided_at)) AS p99_ms,
            max(dateDiff('millisecond', produced_at, decided_at))            AS max_ms
        FROM fraud_detection.transactions
        WHERE account_id != {excluded_account:String}
          AND produced_at >= {start:DateTime64(6)}
          AND produced_at <  {end:DateTime64(6)}
        """,
        parameters={
            "excluded_account": EXCLUDED_ACCOUNT_ID,
            "start":  datetime.fromisoformat(level["start"]),
            "end":    datetime.fromisoformat(level["end"]),
        },
    )
    return result.result_rows[0]

def print_level_report(client, levels: list[dict]) -> list[list]:
    col_widths  = [8, 8, 10, 10, 10, 10, 10]
    header_cols = ["tps", "count", "avg_ms", "p50_ms", "p95_ms", "p99_ms", "max_ms"]
    header = " | ".join(f"{c:>{w}}" for c, w in zip(header_cols, col_widths))

    print("\n[StressTest] Latency by throughput level")
    print(header)
    print("-" * len(header))

    csv_rows = [header_cols]
    for level in levels:
        cnt, avg_ms, p50_ms, p95_ms, p99_ms, max_ms = query_level_latency(client, level)
        if not cnt:
            row = [level["tps"], 0, "-", "-", "-", "-", "-"]
        else:
            row = [level["tps"], cnt, f"{avg_ms:.0f}", f"{p50_ms:.0f}", f"{p95_ms:.0f}", f"{p99_ms:.0f}", f"{max_ms:.0f}"]
        print(" | ".join(f"{str(v):>{w}}" for v, w in zip(row, col_widths)))
        csv_rows.append(row)

    return csv_rows

def write_csv(rows: list[list], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        csv.writer(f).writerows(rows)

def http_get_json(url: str, params: dict | None = None):
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read())

def print_backpressure_report(flink_url: str) -> None:
    print("\n[Backpressure] Querying Flink REST API...")
    try:
        jobs = http_get_json(f"{flink_url}/jobs").get("jobs", [])
        running = [j["id"] for j in jobs if j["status"] == "RUNNING"]
        if not running:
            print("[Backpressure] No running Flink job found -- skipping.")
            return
        job_id = running[0]

        vertices = http_get_json(f"{flink_url}/jobs/{job_id}").get("vertices", [])
        col_widths = [40, 10, 18, 10]
        header = f"{'operator':<{col_widths[0]}} {'busy %':>{col_widths[1]}} " \
                 f"{'backpressured %':>{col_widths[2]}} {'idle %':>{col_widths[3]}}"
        print(header)
        print("-" * len(header))

        for v in vertices:
            metrics = http_get_json(
                f"{flink_url}/jobs/{job_id}/vertices/{v['id']}/metrics",
                params={"get": "busyTimeMsPerSecond,backPressuredTimeMsPerSecond,idleTimeMsPerSecond"},
            )
            values = {m["id"]: float(m["value"]) for m in metrics}
            busy_pct = values.get("busyTimeMsPerSecond", 0.0) / 10
            bp_pct   = values.get("backPressuredTimeMsPerSecond", 0.0) / 10
            idle_pct = values.get("idleTimeMsPerSecond", 0.0) / 10
            flag = "  <-- backpressure!" if bp_pct > 50 else ""
            print(f"{v['name']:<{col_widths[0]}} {busy_pct:>{col_widths[1]-1}.1f}% "
                  f"{bp_pct:>{col_widths[2]-1}.1f}% {idle_pct:>{col_widths[3]-1}.1f}%{flag}")
    except Exception as e:
        print(f"[Backpressure] Could not fetch Flink metrics ({e}) -- skipping. Check manually at {flink_url}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--levels-file", default=LEVELS_INPUT_PATH)
    parser.add_argument("--flink-url", default=FLINK_URL)
    parser.add_argument("--csv-output", default=CSV_OUTPUT_PATH)
    args = parser.parse_args()

    with open(args.levels_file) as f:
        levels = json.load(f)

    client = get_client()
    try:
        csv_rows = print_level_report(client, levels)
    finally:
        client.close()

    write_csv(csv_rows, args.csv_output)
    print(f"\n[StressTest] Results written to {args.csv_output}")

    print_backpressure_report(args.flink_url)


if __name__ == "__main__":
    main()
