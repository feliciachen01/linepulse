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

GOLD_EXPORTS = ["gold_line_status", "gold_alerts", "gold_shift_kpis", "gold_dq_summary", "gold_signal_minute"]


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


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--data", default=str(ROOT / "local_data"))
    p.add_argument("--minutes", type=float, default=480)
    p.add_argument("--chunks", type=int, default=1, help="Split the shift into N incremental pipeline runs")
    p.add_argument("--origin", default="2026-10-19T06:00:00")
    p.add_argument("--keep", action="store_true", help="Do not wipe previous local data")
    p.add_argument("--engine", choices=["delta", "parquet"], default="delta",
                   help="parquet = fallback for machines that cannot download the Delta jar")
    p.add_argument("--sweep", action="store_true", help="Also print the threshold sweep")
    a = p.parse_args(argv)

    data = Path(a.data)
    if data.exists() and not a.keep:
        shutil.rmtree(data)
    spark = local_spark(a.engine)
    store = TableStore.local(spark, str(data), engine=a.engine)
    origin = datetime.fromisoformat(a.origin)

    per_chunk = a.minutes / a.chunks
    timings = []
    for i in range(a.chunks):
        sim = simulator.write_chunk(data, origin, per_chunk)
        t0 = time.time()
        counts = pipeline.run_all(store, str(data / "landing"), use_autoloader=False)
        timings.append(round(time.time() - t0, 1))
        print(f"run {i + 1}/{a.chunks}: +{sim['events']} events -> {json.dumps(counts)} ({timings[-1]} s)")

    export_gold(store, data / "export")
    truth = json.loads((data / "ground_truth" / "faults.json").read_text())
    alerts = store.read("gold_alerts").toPandas()
    data_until = origin + timedelta(minutes=a.minutes)
    score = evaluate.score_alerts(alerts, truth, data_until=data_until)
    dq_truth = json.loads((data / "ground_truth" / "dq_truth.json").read_text())
    quarantined = store.read("silver_quarantine").select("event_id").distinct().count()
    silver_rows = store.read("silver_telemetry").count() + store.read("silver_units").count()
    score["data_quality"] = {
        "injected_bad_records": dq_truth["bad_records"],
        "quarantined_records": quarantined,
        "injected_duplicates": dq_truth["duplicates"],
        "duplicates_removed": store.read("bronze_events").count() - silver_rows - quarantined,
    }
    (data / "export" / "scorecard.json").write_text(json.dumps(score, indent=2, default=str))
    print("\nSCORECARD")
    print(json.dumps(score, indent=2, default=str))
    if a.sweep:
        minutes = store.read("gold_signal_minute").toPandas()
        print("\nTHRESHOLD SWEEP")
        print(evaluate.threshold_sweep(minutes, truth).to_string(index=False))
    spark.stop()


if __name__ == "__main__":
    main()
