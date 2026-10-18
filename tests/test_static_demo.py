import json

from build_static_demo import build


def _export(tmp_path, minutes=3):
    exp = tmp_path / "export"
    exp.mkdir()
    rows = {
        "gold_line_status": [{"line_id": "L2"}, {"line_id": "L1"}],
        "gold_alerts": [{"alert_id": "a", "start_minute": "2026-10-19T08:00:00Z"},
                        {"alert_id": "b", "start_minute": "2026-10-19T12:00:00Z"}],
        "gold_shift_kpis": [{"line_id": "L1", "shift": "A", "shift_date": "2026-10-19"}],
        "gold_dq_summary": [{"rule": "x", "records": 1}, {"rule": "y", "records": 5}],
        "gold_signal_minute": [{"line_id": "L1", "signal": "oven_z1_c", "minute": f"2026-10-19T06:0{i}:00Z"}
                               for i in reversed(range(minutes))],
    }
    for name, data in rows.items():
        (exp / f"{name}.json").write_text(json.dumps(data))
    (exp / "scorecard.json").write_text(json.dumps(
        {"faults": 3, "detected": 3, "mean_latency_min": 8.3, "false_alarms": 0, "per_fault": []}))
    return exp


def test_one_file_per_endpoint_in_api_order(tmp_path):
    out = tmp_path / "site"
    build(_export(tmp_path), out, "https://example.com/repo")
    data = out / "data"
    assert json.loads((data / "health.json").read_text())["mode"] == "snapshot"
    assert [r["line_id"] for r in json.loads((data / "lines.json").read_text())] == ["L1", "L2"]
    assert [r["alert_id"] for r in json.loads((data / "alerts.json").read_text())] == ["b", "a"]  # newest first
    assert [r["rule"] for r in json.loads((data / "data-quality.json").read_text())] == ["y", "x"]
    minutes = [r["minute"] for r in json.loads((data / "signals/L1/oven_z1_c.json").read_text())]
    assert minutes == sorted(minutes)


def test_page_gets_snapshot_metadata_and_only_claims_live_match_for_the_live_run(tmp_path):
    out = tmp_path / "site"
    meta = build(_export(tmp_path, minutes=3), out, "https://example.com/repo")
    page = (out / "index.html").read_text()
    assert "window.LINEPULSE_STATIC" in page and '"matches_live": false' in page
    assert meta["matches_live"] is False and (out / ".nojekyll").exists()
