"""Bakery-line event simulator with injected, ground-truthed faults.

Writes newline-delimited JSON files into a landing folder (a Unity Catalog
volume on Databricks, a local folder otherwise). The simulator is stateless with
respect to time: values are a pure function of (timestamp, seed), so it can be
run in chunks ("the next 15 minutes") and produce the same data as one long run.

Faults are written separately to ground_truth/faults.json so detection can be
scored. Bad records (nulls, sentinel values, duplicates) are injected on purpose
to exercise the silver-layer quarantine.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path

from . import config as C

ISO = "%Y-%m-%dT%H:%M:%S.%f"


@dataclass(frozen=True)
class Fault:
    fault_id: str
    line_id: str
    signal: str          # sensor name, or "fill_weight_g" for the filler
    kind: str            # "ramp" or "step"
    start_min: float     # minutes after simulation origin
    ramp_min: float      # minutes to reach full magnitude (0 for a step)
    hold_min: float      # minutes held at full magnitude before recovering
    magnitude: float     # units of the signal

    def offset(self, t_min: float) -> float:
        """Deviation contributed by this fault at t_min minutes after origin."""
        dt = t_min - self.start_min
        if dt < 0:
            return 0.0
        if self.kind == "step":
            return self.magnitude if dt < self.hold_min else 0.0
        if dt < self.ramp_min:
            return self.magnitude * dt / self.ramp_min
        if dt < self.ramp_min + self.hold_min:
            return self.magnitude
        return 0.0

    @property
    def end_min(self) -> float:
        return self.start_min + self.ramp_min + self.hold_min


DEFAULT_FAULTS = [
    # Slow oven-zone drift: the case supervisors miss until end of shift.
    Fault("F1", "L1", "oven_z3_c", "ramp", start_min=120, ramp_min=20, hold_min=40, magnitude=15.0),
    # Filler creeping heavy: silent giveaway, no rejects at all.
    Fault("F2", "L2", "fill_weight_g", "ramp", start_min=300, ramp_min=15, hold_min=45, magnitude=3.0),
    # Burner fault: sudden drop in zone 1.
    Fault("F3", "L2", "oven_z1_c", "step", start_min=400, ramp_min=0, hold_min=25, magnitude=-12.0),
]


def _rng(seed: int, *parts) -> random.Random:
    return random.Random(f"{seed}|" + "|".join(str(p) for p in parts))


def lot_id_for(line_id: str, origin: datetime, ts: datetime) -> str:
    n = int((ts - origin).total_seconds() // (C.LOT_MINUTES * 60)) + 1
    return f"{line_id}-{origin:%Y%m%d}-{n:03d}"


def wander(seed: int, line_id: str, signal: str, t_min: float) -> float:
    """Slow, smooth, stateless drift around setpoint: three sinusoids with seeded periods.

    Stateless so a chunked run produces exactly the same values as one long run.
    """
    amp = C.WANDER_AMPLITUDE.get(signal, 0.0)
    if amp == 0.0:
        return 0.0
    r = _rng(seed, "wander", line_id, signal)
    total = 0.0
    for _ in range(3):
        period = r.uniform(25, 120)          # minutes
        phase = r.uniform(0, 2 * math.pi)
        total += math.sin(2 * math.pi * t_min / period + phase)
    return amp * total / 3 * 1.6             # ~amp at its typical peaks


def _fault_offset(faults, line_id: str, signal: str, t_min: float) -> float:
    return sum(f.offset(t_min) for f in faults if f.line_id == line_id and f.signal == signal)


def generate_events(origin: datetime, start_min: float, minutes: float, seed: int = 7,
                    faults=DEFAULT_FAULTS, bad_rate: float = 0.002, dup_rate: float = 0.005):
    """Yield event dicts for [start_min, start_min + minutes) after origin."""
    end_min = start_min + minutes
    for line_id, line in C.LINES.items():
        # --- telemetry every TELEMETRY_INTERVAL_S per sensor
        step = C.TELEMETRY_INTERVAL_S / 60.0
        k0 = math.ceil(start_min / step - 1e-9)
        k = k0
        while k * step < end_min - 1e-9:
            t_min = k * step
            ts = origin + timedelta(minutes=t_min)
            for sensor, sp in C.SETPOINTS.items():
                r = _rng(seed, "tel", line_id, sensor, k)
                value = (sp + wander(seed, line_id, sensor, t_min) + r.gauss(0, C.SENSOR_NOISE_SD[sensor])
                         + _fault_offset(faults, line_id, sensor, t_min))
                value = round(value, 2)
                u = r.random()
                if u < bad_rate / 2:
                    value = None                       # dropped reading
                elif u < bad_rate:
                    value = r.choice([-999.0, 9999.0])  # PLC sentinel / wiring fault
                ev = {
                    "event_id": str(uuid.UUID(int=r.getrandbits(128))),
                    "event_type": "telemetry",
                    "plant_id": C.PLANT_ID,
                    "line_id": line_id,
                    "ts": ts.strftime(ISO),
                    "sensor": sensor,
                    "value": value,
                }
                yield ev
                if r.random() < dup_rate:
                    yield dict(ev)  # at-least-once delivery: same event_id re-sent
            k += 1

        # --- one unit event per pack off the checkweigher
        ustep = 1.0 / C.UNITS_PER_MINUTE
        j = math.ceil(start_min / ustep - 1e-9)
        while j * ustep < end_min - 1e-9:
            t_min = j * ustep
            ts = origin + timedelta(minutes=t_min)
            r = _rng(seed, "unit", line_id, j)
            target = (C.TARGET_WEIGHT_G + wander(seed, line_id, "fill_weight_g", t_min)
                      + _fault_offset(faults, line_id, "fill_weight_g", t_min))
            weight = round(r.gauss(target, C.WEIGHT_SD_G), 2)
            # Bake defects get likelier as any oven zone drifts from setpoint.
            worst_dev = max(abs(_fault_offset(faults, line_id, s, t_min)) for s in C.OVEN_SENSORS)
            p_bake = 0.006 + 0.004 * max(0.0, worst_dev - 3.0)
            reason = None
            if weight < C.UNDERWEIGHT_REJECT_G:
                reason = "underweight"
            elif r.random() < p_bake:
                reason = "bake_defect"
            if r.random() < bad_rate:
                weight = 0.0  # load-cell glitch
            ev = {
                "event_id": str(uuid.UUID(int=r.getrandbits(128))),
                "event_type": "unit",
                "plant_id": C.PLANT_ID,
                "line_id": line_id,
                "ts": ts.strftime(ISO),
                "lot_id": lot_id_for(line_id, origin, ts),
                "sku": line["sku"],
                "weight_g": weight,
                "rejected": reason is not None,
                "reject_reason": reason,
            }
            yield ev
            if r.random() < dup_rate:
                yield dict(ev)
            j += 1


def write_chunk(root: str | Path, origin: datetime, minutes: float, seed: int = 7,
                faults=DEFAULT_FAULTS, start_min: float | None = None) -> dict:
    """Append the next `minutes` of events to <root>/landing and update the cursor.

    Returns a small summary dict. Safe to call repeatedly: each call continues
    where the previous one stopped (cursor in <root>/ground_truth/cursor.json).
    """
    root = Path(root)
    landing = root / "landing"
    truth = root / "ground_truth"
    landing.mkdir(parents=True, exist_ok=True)
    truth.mkdir(parents=True, exist_ok=True)
    cursor_path = truth / "cursor.json"

    if start_min is None:
        start_min = json.loads(cursor_path.read_text())["next_min"] if cursor_path.exists() else 0.0

    events = list(generate_events(origin, start_min, minutes, seed=seed, faults=faults))
    # Shuffle within the chunk: real gateways do not deliver in timestamp order.
    _rng(seed, "shuffle", start_min).shuffle(events)

    chunk_start = origin + timedelta(minutes=start_min)
    out = landing / f"events_{chunk_start:%Y%m%dT%H%M%S}.jsonl"
    with out.open("w") as fh:
        for ev in events:
            fh.write(json.dumps(ev) + "\n")

    (truth / "faults.json").write_text(json.dumps({
        "origin": origin.strftime(ISO),
        "faults": [asdict(f) | {"end_min": f.end_min} for f in faults],
    }, indent=2))
    cursor_path.write_text(json.dumps({"origin": origin.strftime(ISO), "next_min": start_min + minutes}))

    # Ground truth for data quality: which records we broke on purpose, and how many re-sends.
    unique = {ev["event_id"]: ev for ev in events}
    bad = [e for e in unique.values() if _is_injected_bad(e)]
    dq_path = truth / "dq_truth.json"
    dq = json.loads(dq_path.read_text()) if dq_path.exists() else {"bad_records": 0, "duplicates": 0, "events": 0}
    dq = {"bad_records": dq["bad_records"] + len(bad),
          "duplicates": dq["duplicates"] + len(events) - len(unique),
          "events": dq["events"] + len(events)}
    dq_path.write_text(json.dumps(dq, indent=2))
    return {"file": str(out), "events": len(events), "from_min": start_min, "to_min": start_min + minutes}


def _is_injected_bad(ev: dict) -> bool:
    if ev["event_type"] == "telemetry":
        return ev["value"] is None or ev["value"] in (-999.0, 9999.0)
    return ev["weight_g"] == 0.0


def main(argv=None):
    p = argparse.ArgumentParser(description="Write simulated bakery-line events.")
    p.add_argument("--root", required=True, help="Folder that will hold landing/ and ground_truth/")
    p.add_argument("--origin", default="2026-10-19T06:00:00", help="Simulation start (shift A)")
    p.add_argument("--minutes", type=float, default=480, help="How many minutes to generate this call")
    p.add_argument("--seed", type=int, default=7)
    a = p.parse_args(argv)
    origin = datetime.fromisoformat(a.origin)
    print(json.dumps(write_chunk(a.root, origin, a.minutes, seed=a.seed)))


if __name__ == "__main__":
    main()
