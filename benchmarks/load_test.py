import argparse
import json
import os
import subprocess
import time
from datetime import datetime, timezone

INFRA_DIR = os.path.join(os.path.dirname(__file__), "..", "infra")
CONTAINER_NAME = "infra-transaction-generator-1"

LEVELS_OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "performance", "load_test_levels.json")


def start_generator() -> None:
    print("[LoadTest] Starting transaction-generator container...")
    subprocess.run(
        ["docker", "compose", "--profile", "seed", "up", "-d", "--build", "transaction-generator"],
        cwd=INFRA_DIR,
        check=True,
    )


def set_tps(tps: int) -> None:
    subprocess.run(
        ["docker", "exec", CONTAINER_NAME, "sh", "-c", f"echo {tps} > /tmp/tps_control"],
        check=True,
    )


def run_level(tps: int, duration_seconds: int) -> dict:
    print(f"\n[LoadTest] level={tps} tps  duration={duration_seconds}s")
    set_tps(tps)
    level_start = datetime.now(timezone.utc)
    time.sleep(duration_seconds)
    level_end = datetime.now(timezone.utc)
    return {"tps": tps, "start": level_start.isoformat(), "end": level_end.isoformat()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--levels", default="500,1000,3000,5000,7000, 10000", help="comma-separated events/s levels")
    parser.add_argument("--duration", type=int, default=60, help="seconds to hold each level")
    parser.add_argument("--output", default=LEVELS_OUTPUT_PATH,
                         help="where to record level time windows for metrics_collector.py")
    args = parser.parse_args()

    levels = [int(x) for x in args.levels.split(",")]

    start_generator()

    results = []
    try:
        for tps in levels:
            results.append(run_level(tps, args.duration))
    except KeyboardInterrupt:
        print("\n[Interrupted]")

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\n[LoadTest] Done. Level windows written to --> {args.output}")
    print("Run metrics_collector.py to compute latency/backpressure per level.")
    print(f"(transaction-generator keeps running -- `make down` or "
          f"`docker compose --profile seed down` in infra/ to stop it.)")


if __name__ == "__main__":
    main()
