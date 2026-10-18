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
