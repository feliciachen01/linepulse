import json
from datetime import datetime

import pandas as pd

from linepulse import config as C, evaluate, pipeline, simulator
from linepulse.simulator import Fault
from linepulse.store import TableStore

ORIGIN = datetime(2026, 10, 19, 6, 0, 0)
STEP = [Fault("T1", "L1", "oven_z2_c", "step", start_min=30, ramp_min=0, hold_min=20, magnitude=12.0)]


def test_dq_rules_name_every_failure(spark):
    base = {f: None for f in pipeline.RAW_SCHEMA.fieldNames()}
    rows = [
        base | dict(event_id="a", event_type="telemetry", line_id="L1", ts="2026-10-19T06:00:00", sensor="oven_z1_c", value=301.0),
        base | dict(event_id="b", event_type="telemetry", line_id="L1", ts="2026-10-19T06:00:00", sensor="oven_z1_c", value=9999.0),
        base | dict(event_id="c", event_type="telemetry", line_id="L9", ts="not a time", sensor="mystery"),
        base | dict(event_id="d", event_type="unit", line_id="L2", ts="2026-10-19T06:00:00", weight_g=0.0),
    ]
    out = {r.event_id: set(r.failed) for r in pipeline.dq_failures(spark.createDataFrame(rows, pipeline.RAW_SCHEMA)).collect()}
    assert out["a"] == set()
    assert out["b"] == {"value_in_physical_bounds"}
    assert out["c"] == {"ts_parseable", "line_known", "sensor_known", "value_present"}
    assert out["d"] == {"lot_present", "weight_in_bounds"}


def _run(spark, root, engine, minutes, chunks=1):
    store = TableStore.local(spark, str(root), engine=engine)
    counts = None
    for _ in range(chunks):
        simulator.write_chunk(root, ORIGIN, minutes / chunks, faults=STEP)
        counts = pipeline.run_all(store, str(root / "landing"), use_autoloader=False)
    return store, counts


def test_end_to_end_detects_step_fault_and_is_idempotent(spark, tmp_path, engine):
    store, counts = _run(spark, tmp_path, engine, minutes=90, chunks=3)

    truth = json.loads((tmp_path / "ground_truth" / "faults.json").read_text())
    alerts = store.read("gold_alerts").toPandas()
    score = evaluate.score_alerts(alerts, truth)
    assert score["detected"] == 1 and score["false_alarms"] == 0
    assert score["per_fault"][0]["latency_min"] <= 6

    # Every alert names the lots produced during the excursion (recall scoping).
    assert all(len(lots) > 0 for lots in alerts["affected_lots"])

    # Bad records are quarantined, duplicates never reach silver.
    dq = json.loads((tmp_path / "ground_truth" / "dq_truth.json").read_text())
    quarantined = store.read("silver_quarantine").select("event_id").distinct().count()
    silver = counts["silver_telemetry"] + counts["silver_units"]
    assert quarantined == dq["bad_records"]
    assert counts["bronze_events"] - silver - quarantined == dq["duplicates"]

    # Re-running with no new files changes nothing (checkpoints + MERGE on event_id).
    again = pipeline.run_all(store, str(tmp_path / "landing"), use_autoloader=False)
    assert again == counts


def test_spark_and_pandas_detectors_agree(spark, tmp_path, engine):
    store, _ = _run(spark, tmp_path, engine, minutes=90)
    minutes = store.read("gold_signal_minute").toPandas()
    d = C.DETECTOR
    from_pandas = evaluate.alerts_from_minutes(minutes, d["alpha"], d["thresholds"], d["min_consecutive"])
    from_spark = store.read("gold_alerts").toPandas()
    assert sorted(from_pandas.alert_id) == sorted(from_spark.alert_id)
    assert list(pd.to_datetime(from_pandas.sort_values("alert_id").detected_at)) == \
        list(pd.to_datetime(from_spark.sort_values("alert_id").detected_at))
