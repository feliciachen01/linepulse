"""Score drift alerts against the simulator's ground-truth faults.

detection latency = detected_at - fault onset (minutes)
false alarm       = an alert that overlaps no fault window on the same line + signal
The threshold sweep re-runs the same EWMA logic in pandas to show the
latency / false-alarm trade-off - the number an interviewer will ask about.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from . import config as C
from .pipeline import ewma_frame

GRACE_MIN = 15  # an alert this long after a fault ends still counts as that fault's recovery tail


def _fault_windows(truth: dict) -> list[dict]:
    origin = datetime.fromisoformat(truth["origin"])
    out = []
    for f in truth["faults"]:
        out.append(f | {"onset": origin + timedelta(minutes=f["start_min"]),
                        "window_end": origin + timedelta(minutes=f["end_min"] + GRACE_MIN)})
    return out


def score_alerts(alerts: pd.DataFrame, truth: dict, data_until: datetime | None = None) -> dict:
    faults = _fault_windows(truth)
    if data_until is not None:  # only score faults that have started in the data seen so far
        faults = [f for f in faults if f["onset"] <= data_until]
    alerts = alerts.copy()
    for c in ("start_minute", "detected_at", "end_minute"):
        alerts[c] = pd.to_datetime(alerts[c])
    matched_alerts = set()
    per_fault = []
    for f in faults:
        m = alerts[(alerts.line_id == f["line_id"]) & (alerts.signal == f["signal"])
                   & (alerts.end_minute >= f["onset"]) & (alerts.start_minute <= f["window_end"])]
        matched_alerts.update(m.alert_id)
        first = m.sort_values("detected_at").head(1)
        latency = None if first.empty else round((first.detected_at.iloc[0] - f["onset"]).total_seconds() / 60, 1)
        per_fault.append({"fault_id": f["fault_id"], "line_id": f["line_id"], "signal": f["signal"],
                          "kind": f["kind"], "magnitude": f["magnitude"], "detected": latency is not None,
                          "latency_min": latency, "alerts": len(m)})
    false_alarms = alerts[~alerts.alert_id.isin(matched_alerts)]
    detected = [p for p in per_fault if p["detected"]]
    return {
        "faults": len(per_fault),
        "detected": len(detected),
        "mean_latency_min": round(sum(p["latency_min"] for p in detected) / len(detected), 1) if detected else None,
        "max_latency_min": max((p["latency_min"] for p in detected), default=None),
        "false_alarms": int(len(false_alarms)),
        "false_alarm_ids": false_alarms.alert_id.tolist(),
        "per_fault": per_fault,
    }


def alerts_from_minutes(minutes: pd.DataFrame, alpha: float, thresholds: dict, k: int) -> pd.DataFrame:
    """Same gaps-and-islands rule as the Spark gold layer, in pandas (for sweeps and tests)."""
    rows = []
    for (line, signal), g in minutes.groupby(["line_id", "signal"]):
        g = ewma_frame(g[["line_id", "signal", "minute", "deviation"]], alpha, thresholds)
        g["minute"] = pd.to_datetime(g["minute"])
        run = []
        for minute, over in zip(g["minute"], g["over"]):
            if over and run and minute - run[-1] == pd.Timedelta(minutes=1):
                run.append(minute)
                continue
            if len(run) >= k:
                rows.append((line, signal, run[0], run[-1]))
            run = [minute] if over else []
        if len(run) >= k:
            rows.append((line, signal, run[0], run[-1]))
    df = pd.DataFrame(rows, columns=["line_id", "signal", "start_minute", "last_over"])
    df["detected_at"] = df["start_minute"] + pd.Timedelta(minutes=k)
    df["end_minute"] = df["last_over"] + pd.Timedelta(minutes=1)
    df["alert_id"] = df["line_id"] + "-" + df["signal"] + "-" + df["start_minute"].dt.strftime("%Y%m%dT%H%M")
    return df


def threshold_sweep(minutes: pd.DataFrame, truth: dict, oven_thresholds=(3, 4, 5, 6, 8),
                    fill_thresholds=(0.6, 1.0, 1.5)) -> pd.DataFrame:
    out = []
    a, k = C.DETECTOR["alpha"], C.DETECTOR["min_consecutive"]
    for ot in oven_thresholds:
        for ft in fill_thresholds:
            thr = {s: ot for s in C.OVEN_SENSORS} | {"fill_weight_g": ft}
            s = score_alerts(alerts_from_minutes(minutes, a, thr, k), truth)
            out.append({"oven_threshold_c": ot, "fill_threshold_g": ft, "detected": s["detected"],
                        "faults": s["faults"], "mean_latency_min": s["mean_latency_min"],
                        "false_alarms": s["false_alarms"]})
    return pd.DataFrame(out)
