"""The gold tables the investigation agent reads, rebuilt in plain pandas straight from events.

Why a second implementation: scoring diagnoses needs hundreds of incidents, i.e. dozens of
simulated days. Spark takes about a minute per day locally; this takes about a second.
tests/test_reference.py runs both on the same events and checks they agree, so the
evaluation measures the agent on exactly the data the Spark pipeline would give it.

Only the tables investigations use are built: signal_minute, quality_minute, alerts.
"""

from __future__ import annotations

import pandas as pd

from . import config as C
from .evaluate import alerts_from_minutes
from .pipeline import ewma_frame


def silver(events: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Same outcome as the silver layer: duplicates removed, records failing DQ rules dropped."""
    df = pd.DataFrame(events).drop_duplicates("event_id")
    df["ts"] = pd.to_datetime(df["ts"])
    tel = df[df.event_type == "telemetry"].dropna(subset=["value"])
    lo = tel.sensor.map({k: v[0] for k, v in C.SENSOR_BOUNDS.items()})
    hi = tel.sensor.map({k: v[1] for k, v in C.SENSOR_BOUNDS.items()})
    tel = tel[(tel.value >= lo) & (tel.value <= hi)]
    units = df[df.event_type == "unit"]
    wlo, whi = C.UNIT_WEIGHT_BOUNDS
    units = units[units.weight_g.between(wlo, whi)].copy()
    units["rejected"] = units["rejected"].astype(bool)
    return tel, units


def signal_minute(tel: pd.DataFrame, units: pd.DataFrame) -> pd.DataFrame:
    t = tel[tel.sensor.isin(C.TELEMETRY_SIGNALS)].assign(minute=lambda d: d.ts.dt.floor("min"))
    sensors = (t.groupby(["line_id", "sensor", "minute"]).value
               .agg(mean_value="mean", n="count", sd_value="std").reset_index()
               .rename(columns={"sensor": "signal"}))
    sensors["setpoint"] = sensors.signal.map(C.SETPOINTS)
    u = units.assign(minute=lambda d: d.ts.dt.floor("min"))
    fill = u.groupby(["line_id", "minute"]).weight_g.agg(mean_value="mean", n="count", sd_value="std").reset_index()
    fill["signal"], fill["setpoint"] = "fill_weight_g", C.TARGET_WEIGHT_G
    m = pd.concat([sensors, fill], ignore_index=True)
    m["deviation"] = m.mean_value - m.setpoint
    a, thr = C.DETECTOR["alpha"], C.DETECTOR["thresholds"]
    return pd.concat([ewma_frame(g, a, thr) for _, g in m.groupby(["line_id", "signal"])], ignore_index=True)


def quality_minute(units: pd.DataFrame) -> pd.DataFrame:
    u = units.assign(minute=lambda d: d.ts.dt.floor("min"),
                     bake=lambda d: d.reject_reason == "bake_defect",
                     under=lambda d: d.reject_reason == "underweight",
                     over_label=lambda d: (d.weight_g - C.LABEL_WEIGHT_G).where(~d.rejected))
    q = (u.groupby(["line_id", "minute"])
         .agg(units=("event_id", "count"), rejects_bake_defect=("bake", "sum"), rejects_underweight=("under", "sum"),
              mean_weight_g=("weight_g", "mean"), giveaway_g_per_pack=("over_label", "mean")).reset_index())
    q[["mean_weight_g", "giveaway_g_per_pack"]] = q[["mean_weight_g", "giveaway_g_per_pack"]].round(3)
    return q


def alerts(sm: pd.DataFrame, units: pd.DataFrame) -> pd.DataFrame:
    k = C.DETECTOR["min_consecutive"]
    a = alerts_from_minutes(sm, C.DETECTOR["alpha"], C.DETECTOR["thresholds"], k)
    if a.empty:
        return a
    data_until = sm.groupby(["line_id", "signal"]).minute.max()
    rows = []
    for r in a.itertuples():
        g = sm[(sm.line_id == r.line_id) & (sm.signal == r.signal)
               & (sm.minute >= r.start_minute) & (sm.minute <= r.last_over)]
        u = units[(units.line_id == r.line_id) & (units.ts >= r.start_minute) & (units.ts < r.end_minute)]
        rows.append({
            "alert_id": r.alert_id, "line_id": r.line_id, "signal": r.signal,
            "direction": "high" if g.ewma_dev.mean() > 0 else "low",
            "status": "open" if r.last_over >= data_until[(r.line_id, r.signal)] else "closed",
            "start_minute": r.start_minute, "detected_at": r.detected_at, "end_minute": r.end_minute,
            "duration_min": int((r.end_minute - r.start_minute).total_seconds() // 60),
            "peak_abs_dev": round(float(g.ewma_dev.abs().max()), 2), "threshold": float(g.threshold.iloc[0]),
            "affected_lots": sorted(u.lot_id.unique().tolist()), "units_in_window": int(len(u)),
            "rejects_in_window": int(u.rejected.sum()),
        })
    return pd.DataFrame(rows).sort_values("start_minute", ignore_index=True)


def iso(df: pd.DataFrame) -> list[dict]:
    """Rows as the JSON export writes them: timestamps as yyyy-MM-ddTHH:mm:ssZ."""
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[c]):
            out[c] = out[c].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return out.astype(object).where(out.notna(), None).to_dict(orient="records")


def gold(events: list[dict]) -> dict[str, list[dict]]:
    """Events -> {gold_signal_minute, gold_quality_minute, gold_alerts} in export format."""
    tel, units = silver(events)
    sm = signal_minute(tel, units)
    return {"gold_signal_minute": iso(sm), "gold_quality_minute": iso(quality_minute(units)),
            "gold_alerts": iso(alerts(sm, units))}
