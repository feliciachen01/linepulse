"""Run the whole LinePulse pipeline on your machine - no Databricks quota used.

    python scripts/run_local.py --minutes 480            # one full shift in one go
    python scripts/run_local.py --minutes 480 --chunks 8 # same shift, 8 incremental runs

Writes Delta tables to local_data/tables, exports gold tables as JSON to
local_data/export (the .NET API reads these in Local mode) and prints the
detection scorecard.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from linepulse import evaluate, pipeline, simulator  # noqa: E402
from linepulse.store import TableStore  # noqa: E402

GOLD_EXPORTS = ["gold_line_status", "gold_alerts", "gold_shift_kpis", "gold_dq_summary", "gold_signal_minute",
                "gold_quality_minute"]


def local_spark(engine: str):
    import os
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)  # workers use this venv too
    from pyspark.sql import SparkSession
    b = (SparkSession.builder.appName("linepulse-local").master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false"))
    if engine == "delta":
        from delta import configure_spark_with_delta_pip
        b = (b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
        b = configure_spark_with_delta_pip(b)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def export_gold(store: TableStore, out_dir: Path) -> None:
    """Write gold tables as JSON arrays - the .NET API's Local mode reads these."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import DateType, TimestampType
    out_dir.mkdir(parents=True, exist_ok=True)
    for t in GOLD_EXPORTS:
        df = store.read(t)
        # Dates as yyyy-MM-dd; timestamps as explicit UTC ("Z"), matching what Databricks SQL returns.
        df = df.select(*[F.date_format(f.name, "yyyy-MM-dd").alias(f.name) if isinstance(f.dataType, DateType)
                         else F.date_format(f.name, "yyyy-MM-dd'T'HH:mm:ss'Z'").alias(f.name)
                         if isinstance(f.dataType, TimestampType) else F.col(f.name) for f in df.schema.fields])
        pdf = df.toPandas()
        (out_dir / f"{t}.json").write_text(pdf.to_json(orient="records", date_format="iso", date_unit="s"))


def run_shift(spark, data: Path, origin: datetime, minutes: float, chunks: int = 1,
              engine: str = "delta", seed: int = 7, verbose: bool = True):
    """Simulate `minutes` of line data in `chunks` incremental runs and score the result.

    Returns (store, score). `data` must be empty or absent.
    """
    store = TableStore.local(spark, str(data), engine=engine)
    per_chunk = minutes / chunks
    for i in range(chunks):
        sim = simulator.write_chunk(data, origin, per_chunk, seed=seed)
        t0 = time.time()
        counts = pipeline.run_all(store, str(data / "landing"), use_autoloader=False)
        if verbose:
            print(f"run {i + 1}/{chunks}: +{sim['events']} events -> {json.dumps(counts)} ({time.time() - t0:.1f} s)")

    truth = json.loads((data / "ground_truth" / "faults.json").read_text())
    alerts = store.read("gold_alerts").toPandas()
    score = evaluate.score_alerts(alerts, truth, data_until=origin + timedelta(minutes=minutes))
    dq_truth = json.loads((data / "ground_truth" / "dq_truth.json").read_text())
    quarantined = store.read("silver_quarantine").select("event_id").distinct().count()
    silver_rows = store.read("silver_telemetry").count() + store.read("silver_units").count()
    score["data_quality"] = {
        "injected_bad_records": dq_truth["bad_records"],
        "quarantined_records": quarantined,
        "injected_duplicates": dq_truth["duplicates"],
        "duplicates_removed": store.read("bronze_events").count() - silver_rows - quarantined,
    }
    return store, score


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--data", default=str(ROOT / "local_data"))
    p.add_argument("--minutes", type=float, default=480)
    p.add_argument("--chunks", type=int, default=1, help="Split the shift into N incremental pipeline runs")
    p.add_argument("--origin", default="2026-10-19T06:00:00")
    p.add_argument("--seed", type=int, default=7, help="Simulator seed (noise and process wander)")
    p.add_argument("--keep", action="store_true", help="Do not wipe previous local data")
    p.add_argument("--engine", choices=["delta", "parquet"], default="delta",
                   help="parquet = fallback for machines that cannot download the Delta jar")
    p.add_argument("--sweep", action="store_true", help="Also print the threshold sweep")
    a = p.parse_args(argv)

    data = Path(a.data)
    if data.exists() and not a.keep:
        shutil.rmtree(data)
    spark = local_spark(a.engine)
    origin = datetime.fromisoformat(a.origin)
    store, score = run_shift(spark, data, origin, a.minutes, a.chunks, a.engine, a.seed)

    export_gold(store, data / "export")
    (data / "export" / "scorecard.json").write_text(json.dumps(score, indent=2, default=str))
    print("\nSCORECARD")
    print(json.dumps(score, indent=2, default=str))
    if a.sweep:
        truth = json.loads((data / "ground_truth" / "faults.json").read_text())
        minutes = store.read("gold_signal_minute").toPandas()
        print("\nTHRESHOLD SWEEP")
        print(evaluate.threshold_sweep(minutes, truth, data_until=origin + timedelta(minutes=a.minutes)).to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
