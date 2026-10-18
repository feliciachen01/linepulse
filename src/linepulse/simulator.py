"""Bakery-line event simulator with injected, ground-truthed faults.

Writes newline-delimited JSON files into a landing folder (a Unity Catalog
volume on Databricks, a local folder otherwise). The simulator is stateless with
respect to time: values are a pure function of (timestamp, seed), so it can be
run in chunks ("the next 15 minutes") and produce the same data as one long run.

Faults are written separately to ground_truth/faults.json so detection (and the
investigation agent's diagnoses) can be scored. Every fault has a root cause, and
each cause leaves a different footprint across signals:

    burner_fault        real heat change in one oven zone   -> bake defects rise
    thermocouple_drift  the reading drifts, the oven doesn't -> no extra defects, noisier reading
    belt_slowdown       belt speed drops                     -> longer bake: defects rise, fewer
                                                                packs per minute, zones read hotter
    filler_drift        pack weight moves                    -> giveaway (heavy) or underweight rejects

So an "oven zone 2 running hot" alert can come from three different causes, and only
the other signals tell them apart. Bad records (nulls, sentinel values, duplicates)
are injected on purpose to exercise the silver-layer quarantine.
"""

from __future__ import annotations

import argparse
import json
import math
import random
import uuid
from dataclasses import asdict, dataclass
from functools import lru_cache
from datetime import datetime, timedelta
from pathlib import Path

from . import config as C

ISO = "%Y-%m-%dT%H:%M:%S.%f"


CAUSES = ("burner_fault", "thermocouple_drift", "belt_slowdown", "filler_drift")


@dataclass(frozen=True)
class Fault:
    fault_id: str
    line_id: str
    signal: str          # the reading the fault moves directly: oven zone, belt speed or fill weight
    kind: str            # "ramp" or "step"
    start_min: float     # minutes after simulation origin
    ramp_min: float      # minutes to reach full magnitude (0 for a step)
    hold_min: float      # minutes held at full magnitude before recovering
    magnitude: float     # units of the signal
    cause: str = ""      # one of CAUSES; empty = the obvious cause for the signal

    @property
    def root_cause(self) -> str:
        if self.cause:
            return self.cause
        if self.signal == "fill_weight_g":
            return "filler_drift"
        if self.signal == "belt_speed_mpm":
            return "belt_slowdown"
        return "burner_fault"

    @property
    def signals(self) -> list[str]:
        """Readings this fault can push over a detector threshold (for scoring alerts)."""
        if self.root_cause == "belt_slowdown":
            return [self.signal, *C.OVEN_SENSORS]
        return [self.signal]

    def active(self, t_min: float) -> bool:
        return self.start_min <= t_min < self.end_min

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
    # Shift A (06:00-14:00). Unchanged since the first release, so shift-A results stay comparable.
    # Burner over-firing in zone 3, creeping up: the case supervisors miss until end of shift.
    Fault("F1", "L1", "oven_z3_c", "ramp", 120, 20, 40, 15.0, "burner_fault"),
    # Filler creeping heavy: silent giveaway, no rejects at all.
    Fault("F2", "L2", "fill_weight_g", "ramp", 300, 15, 45, 3.0, "filler_drift"),
    # Burner flame-out: sudden drop in zone 1.
    Fault("F3", "L2", "oven_z1_c", "step", 400, 0, 25, -12.0, "burner_fault"),
    # Shift B (14:00-22:00). Two lines alarm on a hot oven zone within 30 minutes of each
    # other, for different reasons: a drifting thermocouple on L1, a slowing belt on L2.
    Fault("F4", "L1", "oven_z2_c", "ramp", 600, 25, 45, 11.0, "thermocouple_drift"),
    Fault("F5", "L2", "belt_speed_mpm", "ramp", 630, 8, 40, -1.3, "belt_slowdown"),
    # Shift C (22:00-06:00).
    Fault("F6", "L1", "fill_weight_g", "ramp", 900, 15, 40, -2.6, "filler_drift"),
    Fault("F7", "L2", "oven_z3_c", "ramp", 1080, 20, 40, -10.0, "thermocouple_drift"),
    Fault("F8", "L1", "belt_speed_mpm", "ramp", 1260, 8, 35, -1.1, "belt_slowdown"),
]


def random_faults(seed: int, minutes: float = 1440, per_day: int = 8) -> list[Fault]:
    """A random day of incidents for evaluating diagnoses: causes, lines, zones, directions,
    shapes and sizes all drawn from the seed. At most one incident per line at a time, with
    quiet time in between so every alert belongs to exactly one incident."""
    r = _rng(seed, "incidents")
    n = max(1, round(per_day * minutes / 1440))
    gap = minutes / n
    out = []
    for i in range(n):
        line = r.choice(sorted(C.LINES))
        cause = CAUSES[r.randrange(len(CAUSES))]
        start = round(i * gap + r.uniform(20, max(21, gap - 75)))
        hold = r.randint(25, 45)
        sign = r.choice([-1, 1])
        if cause == "burner_fault":
            kind = r.choice(["step", "ramp"])
            f = (r.choice(C.OVEN_SENSORS), kind, 0 if kind == "step" else r.randint(8, 20), sign * r.uniform(9, 16))
        elif cause == "thermocouple_drift":
            f = (r.choice(C.OVEN_SENSORS), "ramp", r.randint(12, 30), sign * r.uniform(8, 14))
        elif cause == "belt_slowdown":
            f = ("belt_speed_mpm", "ramp", r.randint(4, 10), -r.uniform(0.9, 1.6))
        else:
            f = ("fill_weight_g", "ramp", r.randint(8, 20), sign * r.uniform(2.0, 3.5))
        signal, kind, ramp, mag = f
        out.append(Fault(f"R{i + 1}", line, signal, kind, start, ramp, hold, round(mag, 2), cause))
    # Two incidents on the same line must not overlap (keeps ground truth unambiguous).
    kept, busy_until = [], {}
    for f in sorted(out, key=lambda f: f.start_min):
        if f.start_min >= busy_until.get(f.line_id, -1e9) + 30 and f.end_min <= minutes:
            kept.append(f)
            busy_until[f.line_id] = f.end_min
    return kept


def _rng(seed: int, *parts) -> random.Random:
    return random.Random(f"{seed}|" + "|".join(str(p) for p in parts))


def lot_id_for(line_id: str, origin: datetime, ts: datetime) -> str:
    n = int((ts - origin).total_seconds() // (C.LOT_MINUTES * 60)) + 1
    return f"{line_id}-{origin:%Y%m%d}-{n:03d}"


@lru_cache(maxsize=None)
def _wander_terms(seed: int, line_id: str, signal: str) -> tuple[tuple[float, float], ...]:
    r = _rng(seed, "wander", line_id, signal)
    terms = []
    for _ in range(3):
        period = r.uniform(25, 120)          # minutes
        phase = r.uniform(0, 2 * math.pi)
        terms.append((period, phase))
    return tuple(terms)


def wander(seed: int, line_id: str, signal: str, t_min: float) -> float:
    """Slow, smooth, stateless drift around setpoint: three sinusoids with seeded periods.

    Stateless so a chunked run produces exactly the same values as one long run.
    """
    amp = C.WANDER_AMPLITUDE.get(signal, 0.0)
    if amp == 0.0:
        return 0.0
    total = 0.0
    for period, phase in _wander_terms(seed, line_id, signal):
        total += math.sin(2 * math.pi * t_min / period + phase)
    return amp * total / 3 * 1.6             # ~amp at its typical peaks


def _fault_offset(faults, line_id: str, signal: str, t_min: float) -> float:
    """How far the *reading* of a signal is moved by faults (whatever their cause)."""
    direct = sum(f.offset(t_min) for f in faults if f.line_id == line_id and f.signal == signal)
    if signal in C.OVEN_SENSORS:
        direct += _belt_load_heat(faults, line_id, t_min)
    return direct


def _belt_slowdown(faults, line_id: str, t_min: float) -> float:
    """Metres per minute the belt is running below setpoint (>= 0)."""
    return -sum(f.offset(t_min) for f in faults if f.line_id == line_id and f.root_cause == "belt_slowdown")


def _belt_load_heat(faults, line_id: str, t_min: float) -> float:
    """A slower belt carries less dough through the oven, so every zone runs hotter."""
    return C.OVEN_HEAT_PER_MPM_SLOWDOWN * _belt_slowdown(faults, line_id, t_min)


def _actual_oven_dev(faults, line_id: str, sensor: str, t_min: float) -> float:
    """What the product actually experiences: a drifting thermocouple changes the reading only."""
    real = sum(f.offset(t_min) for f in faults
               if f.line_id == line_id and f.signal == sensor and f.root_cause != "thermocouple_drift")
    return real + _belt_load_heat(faults, line_id, t_min)


def _noise_factor(faults, line_id: str, sensor: str, t_min: float) -> float:
    """A failing thermocouple reads noisier as well as wrong."""
    return C.FAILING_SENSOR_NOISE_FACTOR if any(
        f.line_id == line_id and f.signal == sensor and f.root_cause == "thermocouple_drift" and f.active(t_min)
        for f in faults) else 1.0


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
                noise = r.gauss(0, C.SENSOR_NOISE_SD[sensor]) * _noise_factor(faults, line_id, sensor, t_min)
                value = sp + wander(seed, line_id, sensor, t_min) + noise + _fault_offset(faults, line_id, sensor, t_min)
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
            # Bake defects get likelier as the real oven temperature drifts from setpoint and
            # as the belt slows (longer time in the oven). A drifting sensor changes neither.
            worst_dev = max(abs(_actual_oven_dev(faults, line_id, s, t_min)) for s in C.OVEN_SENSORS)
            slowdown = _belt_slowdown(faults, line_id, t_min)
            p_bake = (0.006 + 0.004 * max(0.0, worst_dev - 3.0)
                      + C.BAKE_DEFECT_PER_MPM_SLOWDOWN * max(0.0, slowdown - 0.2))
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
            dup = r.random() < dup_rate
            # A slower belt delivers fewer packs per minute (drawn last: no effect outside a slowdown).
            if slowdown > 0 and r.random() > (C.SETPOINTS["belt_speed_mpm"] - slowdown) / C.SETPOINTS["belt_speed_mpm"]:
                j += 1
                continue
            yield ev
            if dup:
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
        "faults": [asdict(f) | {"cause": f.root_cause, "signals": f.signals, "end_min": f.end_min} for f in faults],
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
    p.add_argument("--random-faults", action="store_true", help="A random day of incidents instead of the default day")
    a = p.parse_args(argv)
    origin = datetime.fromisoformat(a.origin)
    faults = random_faults(a.seed) if a.random_faults else DEFAULT_FAULTS
    print(json.dumps(write_chunk(a.root, origin, a.minutes, seed=a.seed, faults=faults)))


if __name__ == "__main__":
    main()
