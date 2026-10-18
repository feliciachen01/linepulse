"""Score detection across several simulated shifts instead of one.

One shift with three faults is an anecdote. This runs the full pipeline once per
simulator seed (different sensor noise and process wander, same injected faults)
and reports latency and false alarms as distributions, plus a threshold sweep
summed over every shift.

    python scripts/evaluate_seeds.py --seeds 10           # ~5 min on a laptop
    python scripts/evaluate_seeds.py --seeds 3 --engine parquet

Writes local_data/seed_eval/summary.json and summary.md (paste the table into the README).
"""

from __future__ import annotations

import argparse
import json
import shutil
import statistics
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import pandas as pd  # noqa: E402

from linepulse import config as C, evaluate  # noqa: E402
from run_local import local_spark, run_shift  # noqa: E402


def pct(values, q):
    """Nearest-rank percentile; fine for the handful of values we have."""
    v = sorted(values)
    return v[min(len(v) - 1, max(0, round(q / 100 * len(v)) - 1))]


def summarize(scores: list[dict]) -> dict:
    latencies = [f["latency_min"] for s in scores for f in s["per_fault"] if f["detected"]]
    faults = sum(s["faults"] for s in scores)
    detected = sum(s["detected"] for s in scores)
    fa = [s["false_alarms"] for s in scores]
    by_fault = {}
    for s in scores:
        for f in s["per_fault"]:
            by_fault.setdefault(f["fault_id"], []).append(f["latency_min"])
    return {
        "shifts": len(scores),
        "faults": faults,
        "detected": detected,
        "detection_rate": round(detected / faults, 3) if faults else None,
        "latency_min": {"mean": round(statistics.mean(latencies), 1), "p50": pct(latencies, 50),
                        "p90": pct(latencies, 90), "max": max(latencies)} if latencies else None,
        "latency_by_fault": {k: {"min": min(x for x in v if x is not None) if any(x is not None for x in v) else None,
                                 "max": max(x for x in v if x is not None) if any(x is not None for x in v) else None,
                                 "missed": sum(x is None for x in v)} for k, v in by_fault.items()},
        "false_alarms_per_shift": {"mean": round(statistics.mean(fa), 2), "max": max(fa), "total": sum(fa)},
        "quarantine_exact_every_shift": all(s["data_quality"]["injected_bad_records"] == s["data_quality"]["quarantined_records"] for s in scores),
        "dedup_exact_every_shift": all(s["data_quality"]["injected_duplicates"] == s["data_quality"]["duplicates_removed"] for s in scores),
    }


def to_markdown(summary: dict, sweep: pd.DataFrame, seeds: list[int]) -> str:
    lat = summary["latency_min"]
    lines = [
        f"**{summary['shifts']} simulated shifts** (seeds {seeds[0]}-{seeds[-1]}), same 3 injected faults, different noise and wander:",
        "",
        "| Metric | Result |",
        "| --- | --- |",
        f"| Faults detected | {summary['detected']} / {summary['faults']} ({summary['detection_rate']:.0%}) |",
        f"| Detection latency | mean {lat['mean']} min, p50 {lat['p50']}, p90 {lat['p90']}, max {lat['max']} |",
        f"| False alarms per shift | mean {summary['false_alarms_per_shift']['mean']}, max {summary['false_alarms_per_shift']['max']} |",
        f"| Quarantine and dedup exact in every shift | {'yes' if summary['quarantine_exact_every_shift'] and summary['dedup_exact_every_shift'] else 'no'} |",
        "",
        "| Fault | Fastest | Slowest | Missed |",
        "| --- | --- | --- | --- |",
    ]
    for k, v in summary["latency_by_fault"].items():
        lines.append(f"| {k} | {v['min']} min | {v['max']} min | {v['missed']} |")
    lines += ["", "Threshold sweep, summed over all shifts:", "",
              "| Oven threshold (°C) | Fill threshold (g) | Detected | Mean latency (min) | False alarms (total) |",
              "| --- | --- | --- | --- | --- |"]
    for r in sweep.itertuples():
        lines.append(f"| {r.oven_threshold_c} | {r.fill_threshold_g} | {r.detected}/{r.faults} | {r.mean_latency_min} | {r.false_alarms} |")
    return "\n".join(lines) + "\n"


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--seeds", type=int, default=10, help="Number of shifts (seeds 1..N)")
    p.add_argument("--minutes", type=float, default=480)
    p.add_argument("--engine", choices=["delta", "parquet"], default="delta")
    p.add_argument("--out", default=str(ROOT / "local_data" / "seed_eval"))
    a = p.parse_args(argv)

    out = Path(a.out)
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    spark = local_spark(a.engine)
    origin = datetime.fromisoformat("2026-10-19T06:00:00")
    seeds = list(range(1, a.seeds + 1))

    scores, sweeps = [], []
    for seed in seeds:
        store, score = run_shift(spark, out / f"seed_{seed}", origin, a.minutes, engine=a.engine, seed=seed, verbose=False)
        truth = json.loads((out / f"seed_{seed}" / "ground_truth" / "faults.json").read_text())
        sweeps.append(evaluate.threshold_sweep(store.read("gold_signal_minute").toPandas(), truth))
        scores.append(score)
        print(f"seed {seed}: detected {score['detected']}/{score['faults']}, "
              f"latency {[f['latency_min'] for f in score['per_fault']]}, false alarms {score['false_alarms']}")

    keys = ["oven_threshold_c", "fill_threshold_g"]
    allsw = pd.concat(sweeps)
    sweep = (allsw.groupby(keys, as_index=False)
             .agg(detected=("detected", "sum"), faults=("faults", "sum"),
                  mean_latency_min=("mean_latency_min", "mean"), false_alarms=("false_alarms", "sum")))
    sweep["mean_latency_min"] = sweep["mean_latency_min"].round(1)

    summary = summarize(scores)
    summary["defaults"] = {"oven_threshold_c": C.DETECTOR["thresholds"]["oven_z1_c"],
                           "fill_threshold_g": C.DETECTOR["thresholds"]["fill_weight_g"]}
    (out / "summary.json").write_text(json.dumps({"summary": summary, "per_seed": scores,
                                                  "sweep": sweep.to_dict(orient="records")}, indent=2, default=str))
    md = to_markdown(summary, sweep, seeds)
    (out / "summary.md").write_text(md)
    print("\n" + md)
    spark.stop()


if __name__ == "__main__":
    main()
