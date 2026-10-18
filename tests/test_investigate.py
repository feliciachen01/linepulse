"""Incident-investigation agent: evidence tools, deterministic investigator, LLM loop (fake server)."""

import json
from dataclasses import asdict
from datetime import datetime

import pytest

from linepulse import reference, simulator
from linepulse.investigate import IncidentTools, investigate, match_truth, rules_investigation
from linepulse.simulator import Fault

ORIGIN = datetime(2026, 10, 19, 6, 0, 0)
# Both lines alarm on a hot oven at about the same time: a drifting sensor on L1, a slow belt on L2.
FAULTS = [Fault("T", "L1", "oven_z2_c", "ramp", 80, 10, 50, 11.0, "thermocouple_drift"),
          Fault("B", "L2", "belt_speed_mpm", "ramp", 85, 5, 45, -1.4, "belt_slowdown")]
TRUTH = {"origin": ORIGIN.isoformat(), "faults": [asdict(f) | {"cause": f.root_cause, "signals": f.signals,
                                                               "end_min": f.end_min} for f in FAULTS]}


@pytest.fixture(scope="module")
def tools():
    return IncidentTools(reference.gold(list(simulator.generate_events(ORIGIN, 0, 160, faults=FAULTS))))


def first_alert(tools, line, prefix):
    a = tools.alerts[(tools.alerts.line_id == line) & tools.alerts.signal.str.startswith(prefix)]
    return a.sort_values("start_minute").alert_id.iloc[0]


def test_evidence_separates_a_drifting_sensor_from_a_slow_belt(tools):
    tc = first_alert(tools, "L1", "oven_")
    a = tools.get_alert(tc)
    q = tools.get_product_quality("L1", a["window_start"], a["window_end"])
    s = tools.get_signal_window("L1", "oven_z2_c", a["window_start"], a["window_end"])
    assert q["bake_defect_rate_ratio"] < 1.6 and s["noise_ratio"] >= 2.0     # product fine, reading noisy

    belt_zone = first_alert(tools, "L2", "oven_")
    a = tools.get_alert(belt_zone)
    c = {x["signal"]: x for x in tools.compare_line_signals("L2", a["window_start"], a["window_end"])["signals"]}
    q = tools.get_product_quality("L2", a["window_start"], a["window_end"])
    assert c["belt_speed_mpm"]["change"] <= -1.0 and q["packs_per_min_change_pct"] <= -15
    assert tools.get_related_alerts(belt_zone)["other_lines_same_time"]          # L1 alarmed too


def test_rules_investigator_names_the_injected_cause_and_every_figure_checks(tools):
    for line, prefix, cause in [("L1", "oven_", "thermocouple_drift"), ("L2", "oven_", "belt_slowdown"),
                                ("L2", "belt_", "belt_slowdown")]:
        alert_id = first_alert(tools, line, prefix)
        rec = investigate(tools, alert_id, None, TRUTH, log=lambda *a: None)
        assert rec["diagnosis"]["root_cause"] == cause == rec["ground_truth"]["cause"]
        assert rec["status"] == "verified" and rec["figures_total"] >= 5 and rec["writer"] == "rules"
        assert [s["tool"] for s in rec["steps"]] == ["get_alert", "get_signal_window", "compare_line_signals",
                                                     "get_product_quality", "get_related_alerts"]
    # The lookup table an alarm list implies gets the drifting sensor wrong.
    assert investigate(tools, first_alert(tools, "L1", "oven_"), None, log=lambda *a: None)["baseline_lookup"] == "burner_fault"


def test_match_truth_ignores_other_lines_and_signals(tools):
    a = tools.alerts[tools.alerts.alert_id == first_alert(tools, "L1", "oven_")].iloc[0].to_dict()
    assert match_truth(a, TRUTH)["fault_id"] == "T"
    assert match_truth(a | {"line_id": "L2"}, {"origin": TRUTH["origin"], "faults": TRUTH["faults"][:1]}) is None


def _call(name, args, cid="c"):
    return {"id": cid, "type": "function", "function": {"name": name, "arguments": json.dumps(args)}}


def _diagnosis(tools, alert_id, **over):
    a = tools.get_alert(alert_id)
    q = tools.get_product_quality(a["line_id"], a["window_start"], a["window_end"])
    d = {"root_cause": "thermocouple_drift", "confidence": "high",
         "summary": f"{a['line_id']} oven zone 2 reads {a['peak_smoothed_deviation']} °C high but the product is unchanged.",
         "evidence": [f"Bake defects {q['during']['bake_defects_per_1000']} per 1,000 packs vs "
                      f"{q['before']['bake_defects_per_1000']} before."],
         "ruled_out": [{"cause": "burner_fault", "why": "No rise in bake defects."}],
         "actions": ["Check zone 2 with a handheld probe."]}
    return d | over


def test_llm_agent_is_recorded_step_by_step_and_verified(fake_llm, tools):
    alert_id = first_alert(tools, "L1", "oven_")
    a = tools.get_alert(alert_id)
    w = {"line_id": "L1", "start": a["window_start"], "end": a["window_end"]}
    fake_llm["replies"] = [
        {"role": "assistant", "content": "First, the alert.", "tool_calls": [_call("get_alert", {"alert_id": alert_id})]},
        {"role": "assistant", "content": "Did the product change?", "tool_calls": [_call("get_product_quality", w)]},
        {"role": "assistant", "content": json.dumps(_diagnosis(tools, alert_id))},
    ]
    rec = investigate(tools, alert_id, fake_llm["client"], TRUTH, log=lambda *a: None)
    assert rec["writer"] == "llm" and rec["status"] == "verified" and not rec["retried"]
    assert [(s["thought"], s["tool"]) for s in rec["steps"]] == [("First, the alert.", "get_alert"),
                                                                 ("Did the product change?", "get_product_quality")]
    assert rec["diagnosis"]["root_cause"] == rec["ground_truth"]["cause"]
    sent = fake_llm["requests"][0]["body"]
    assert {t["function"]["name"] for t in sent["tools"]} >= {"compare_line_signals", "get_signal_window"}


def test_invented_figure_is_sent_back_once_then_corrected(fake_llm, tools):
    alert_id = first_alert(tools, "L1", "oven_")
    bad = _diagnosis(tools, alert_id, summary="Zone 2 reads 23.4 °C high; the product is unchanged.")
    fake_llm["replies"] = [
        {"role": "assistant", "content": None, "tool_calls": [_call("get_alert", {"alert_id": alert_id})]},
        {"role": "assistant", "content": None, "tool_calls": [_call("get_product_quality", {
            "line_id": "L1", "start": tools.get_alert(alert_id)["window_start"],
            "end": tools.get_alert(alert_id)["window_end"]})]},
        {"role": "assistant", "content": "```json\n" + json.dumps(bad) + "\n```"},
        {"role": "assistant", "content": json.dumps(_diagnosis(tools, alert_id))},
    ]
    rec = investigate(tools, alert_id, fake_llm["client"], log=lambda *a: None)
    assert rec["retried"] and rec["status"] == "verified"
    assert "23.4" in fake_llm["requests"][-1]["body"]["messages"][-1]["content"]


def test_bad_cause_or_api_failure_falls_back_to_rules(fake_llm, tools):
    alert_id = first_alert(tools, "L1", "oven_")
    fake_llm["replies"] = [{"role": "assistant", "content": "I think it is gremlins."},
                           {"role": "assistant", "content": "Still gremlins."}]
    rec = investigate(tools, alert_id, fake_llm["client"], log=lambda *a: None)
    assert rec["writer"] == "rules" and "JSON" in rec["llm_error"]
    fake_llm["status"] = 401
    rec = investigate(tools, alert_id, fake_llm["client"], log=lambda *a: None)
    assert rec["writer"] == "rules" and "401" in rec["llm_error"]
    assert rec["diagnosis"]["root_cause"] == rules_investigation(tools, alert_id)["diagnosis"]["root_cause"]
