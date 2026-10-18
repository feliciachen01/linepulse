from evaluate_seeds import pct, summarize


def _score(latencies, false_alarms, bad=10, quarantined=10, dups=5, removed=5):
    per_fault = [{"fault_id": f"F{i + 1}", "detected": lat is not None, "latency_min": lat}
                 for i, lat in enumerate(latencies)]
    return {"faults": len(latencies), "detected": sum(lat is not None for lat in latencies),
            "false_alarms": false_alarms, "per_fault": per_fault,
            "data_quality": {"injected_bad_records": bad, "quarantined_records": quarantined,
                             "injected_duplicates": dups, "duplicates_removed": removed}}


def test_percentile_nearest_rank():
    assert pct([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 50) == 5
    assert pct([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 90) == 9
    assert pct([7], 90) == 7


def test_summarize_counts_misses_and_false_alarms():
    s = summarize([_score([10, 8, 3], 0), _score([None, 9, 4], 2, quarantined=9)])
    assert (s["faults"], s["detected"]) == (6, 5)
    assert s["latency_min"]["max"] == 10
    assert s["latency_by_fault"]["F1"] == {"min": 10, "max": 10, "missed": 1}
    assert s["false_alarms_per_shift"] == {"mean": 1, "max": 2, "total": 2}
    assert s["quarantine_exact_every_shift"] is False
    assert s["dedup_exact_every_shift"] is True
