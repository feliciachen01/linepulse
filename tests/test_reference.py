"""The pandas reference gold (used to score diagnoses over many days) must match Spark gold."""

from datetime import datetime

import pandas as pd

from linepulse import pipeline, reference, simulator
from linepulse.simulator import Fault
from linepulse.store import TableStore

ORIGIN = datetime(2026, 10, 19, 6, 0, 0)
FAULTS = [Fault("B", "L2", "belt_speed_mpm", "ramp", 20, 5, 25, -1.4, "belt_slowdown"),
          Fault("T", "L1", "oven_z1_c", "ramp", 30, 10, 30, 12.0, "thermocouple_drift")]


def test_reference_gold_matches_spark(spark, tmp_path, engine):
    simulator.write_chunk(tmp_path, ORIGIN, 90, faults=FAULTS)
    store = TableStore.local(spark, str(tmp_path), engine=engine)
    pipeline.run_all(store, str(tmp_path / "landing"), use_autoloader=False)
    ref = reference.gold(list(simulator.generate_events(ORIGIN, 0, 90, faults=FAULTS)))

    def spark_rows(table):
        df = store.read(table).toPandas()
        return reference.iso(df)

    a = pd.DataFrame(ref["gold_alerts"]).set_index("alert_id").sort_index()
    b = pd.DataFrame(spark_rows("gold_alerts")).set_index("alert_id").sort_index()
    assert len(a) >= 2 and list(a.index) == list(b.index)
    for c in ["direction", "status", "start_minute", "end_minute", "duration_min", "units_in_window", "rejects_in_window"]:
        assert a[c].tolist() == b[c].tolist(), c
    assert (a.peak_abs_dev - b.peak_abs_dev).abs().max() < 1e-6
    assert a.affected_lots.map(list).tolist() == b.affected_lots.map(list).tolist()

    key = ["line_id", "signal", "minute"]
    a = pd.DataFrame(ref["gold_signal_minute"]).sort_values(key, ignore_index=True)
    b = pd.DataFrame(spark_rows("gold_signal_minute")).sort_values(key, ignore_index=True)
    assert a[key].equals(b[key])
    for c in ["mean_value", "sd_value", "ewma_dev"]:
        assert (a[c] - b[c]).abs().max() < 1e-6, c

    key = ["line_id", "minute"]
    a = pd.DataFrame(ref["gold_quality_minute"]).sort_values(key, ignore_index=True)
    b = pd.DataFrame(spark_rows("gold_quality_minute")).sort_values(key, ignore_index=True)
    for c in ["units", "rejects_bake_defect", "rejects_underweight"]:
        assert a[c].tolist() == b[c].tolist(), c
    assert (a.mean_weight_g - b.mean_weight_g).abs().max() <= 0.0011  # rounding ties may differ
