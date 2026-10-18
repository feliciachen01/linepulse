from datetime import datetime

import pandas as pd

from linepulse import evaluate

TRUTH = {"origin": "2026-10-19T06:00:00",
         "faults": [{"fault_id": "F1", "line_id": "L1", "signal": "oven_z1_c", "kind": "step",
                     "magnitude": 10.0, "start_min": 120, "end_min": 140}]}


def _quiet_minutes(n=60):
    """n minutes of a signal sitting on setpoint: no alert at any threshold."""
    return pd.DataFrame({"line_id": "L1", "signal": "oven_z1_c",
                         "minute": pd.date_range("2026-10-19 06:00", periods=n, freq="min"),
                         "deviation": 0.1})


def test_no_alerts_gives_an_empty_frame_not_an_error():
    alerts = evaluate.alerts_from_minutes(_quiet_minutes(), 0.3, {"oven_z1_c": 5.0}, 3)
    assert alerts.empty and "alert_id" in alerts.columns


def test_sweep_before_first_fault_scores_nothing_as_missed():
    # The first hour of a shift (notebook 01's default chunk) ends before F1 starts.
    sweep = evaluate.threshold_sweep(_quiet_minutes(), TRUTH, data_until=datetime(2026, 10, 19, 7, 0))
    assert (sweep["faults"] == 0).all() and (sweep["false_alarms"] == 0).all()


def test_sweep_without_cutoff_counts_the_future_fault_as_missed():
    sweep = evaluate.threshold_sweep(_quiet_minutes(), TRUTH)
    assert (sweep["faults"] == 1).all() and (sweep["detected"] == 0).all()
