from datetime import datetime

from linepulse import simulator
from linepulse.simulator import Fault

ORIGIN = datetime(2026, 10, 19, 6, 0, 0)


def test_chunked_generation_matches_single_run():
    one = list(simulator.generate_events(ORIGIN, 0, 20))
    two = list(simulator.generate_events(ORIGIN, 0, 10)) + list(simulator.generate_events(ORIGIN, 10, 10))
    key = lambda e: (e["event_id"], e["ts"])
    assert sorted(map(key, one)) == sorted(map(key, two))


def test_ramp_and_step_offsets():
    ramp = Fault("R", "L1", "oven_z3_c", "ramp", start_min=10, ramp_min=20, hold_min=5, magnitude=10)
    assert ramp.offset(5) == 0
    assert ramp.offset(20) == 5            # halfway up the ramp
    assert ramp.offset(32) == 10           # holding
    assert ramp.offset(36) == 0            # recovered
    step = Fault("S", "L2", "oven_z1_c", "step", start_min=0, ramp_min=0, hold_min=3, magnitude=-12)
    assert step.offset(1) == -12 and step.offset(3) == 0


def test_lot_ids_roll_every_30_minutes():
    assert simulator.lot_id_for("L1", ORIGIN, ORIGIN) == "L1-20261019-001"
    assert simulator.lot_id_for("L1", ORIGIN, datetime(2026, 10, 19, 6, 31)) == "L1-20261019-002"


def test_write_chunk_records_ground_truth(tmp_path):
    simulator.write_chunk(tmp_path, ORIGIN, 5)
    simulator.write_chunk(tmp_path, ORIGIN, 5)
    assert len(list((tmp_path / "landing").glob("*.jsonl"))) == 2
    assert (tmp_path / "ground_truth" / "faults.json").exists()
    import json
    dq = json.loads((tmp_path / "ground_truth" / "dq_truth.json").read_text())
    assert dq["events"] > 0 and dq["duplicates"] >= 0


def _units(faults, start, minutes, line="L1"):
    evs = [e for e in simulator.generate_events(ORIGIN, start, minutes, faults=faults)
           if e["event_type"] == "unit" and e["line_id"] == line]
    return len({e["event_id"] for e in evs}), sum(e["reject_reason"] == "bake_defect" for e in evs)


def _mean_reading(faults, sensor, start, minutes, line="L1"):
    vals = [e["value"] for e in simulator.generate_events(ORIGIN, start, minutes, faults=faults)
            if e["event_type"] == "telemetry" and e["line_id"] == line and e["sensor"] == sensor
            and e["value"] is not None and 0 < e["value"] < 1000]
    return sum(vals) / len(vals)


def test_root_causes_leave_different_footprints():
    """Same reading (+12 °C in zone 2), different causes: only a real heat change hurts product."""
    burner = [Fault("B", "L1", "oven_z2_c", "step", 0, 0, 60, 12.0, "burner_fault")]
    sensor = [Fault("T", "L1", "oven_z2_c", "step", 0, 0, 60, 12.0, "thermocouple_drift")]
    assert abs(_mean_reading(burner, "oven_z2_c", 0, 30) - _mean_reading(sensor, "oven_z2_c", 0, 30)) < 1.0
    _, defects_burner = _units(burner, 0, 30)
    _, defects_sensor = _units(sensor, 0, 30)
    _, defects_normal = _units([], 0, 30)
    assert defects_burner > 3 * defects_normal
    assert defects_sensor == defects_normal          # identical product: the oven never changed


def test_belt_slowdown_cuts_throughput_and_heats_every_zone():
    belt = [Fault("S", "L1", "belt_speed_mpm", "step", 0, 0, 60, -1.2, "belt_slowdown")]
    packs_slow, defects_slow = _units(belt, 0, 30)
    packs_normal, defects_normal = _units([], 0, 30)
    assert 0.75 < packs_slow / packs_normal < 0.85   # 4.8 / 6.0 m/min
    assert defects_slow > 3 * defects_normal
    for zone in ("oven_z1_c", "oven_z2_c", "oven_z3_c"):
        assert _mean_reading(belt, zone, 0, 30) - _mean_reading([], zone, 0, 30) > 4.0
    assert belt[0].signals == ["belt_speed_mpm", "oven_z1_c", "oven_z2_c", "oven_z3_c"]


def test_random_faults_are_reproducible_and_never_overlap_on_a_line():
    a, b = simulator.random_faults(3), simulator.random_faults(3)
    assert a == b and len(a) >= 6
    assert {f.root_cause for seed in range(1, 6) for f in simulator.random_faults(seed)} == set(simulator.CAUSES)
    for line in ("L1", "L2"):
        fs = sorted((f for f in a if f.line_id == line), key=lambda f: f.start_min)
        assert all(x.end_min <= y.start_min for x, y in zip(fs, fs[1:]))
