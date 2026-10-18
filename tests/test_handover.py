"""Shift-handover agent tests, against a fake OpenAI-compatible server (no API key, no network)."""

import json
import pytest

from linepulse.handover import GoldTools, generate, template_note, verify

GOLD = {
    "gold_shift_kpis": [
        {"line_id": "L1", "shift_date": "2026-10-19", "shift": "A", "units": 28737, "rejects": 332,
         "scrap_rate_pct": 1.155, "rejects_underweight": 24, "rejects_bake_defect": 308,
         "avg_accepted_weight_g": 91.99, "giveaway_g_per_pack": 1.988, "giveaway_kg": 56.48,
         "giveaway_cost_cad": 141.2, "alerts": 1},
    ],
    "gold_alerts": [
        {"alert_id": "x", "line_id": "L1", "signal": "oven_z3_c", "direction": "high", "status": "closed",
         "start_minute": "2026-10-19T08:09:00Z", "detected_at": "2026-10-19T08:12:00Z",
         "end_minute": "2026-10-19T09:02:00Z", "duration_min": 53, "peak_abs_dev": 17.27, "threshold": 5.0,
         "affected_lots": ["L1-20261019-005", "L1-20261019-006"], "units_in_window": 3172, "rejects_in_window": 165},
    ],
    "gold_line_status": [{"line_id": "L1", "product": "Garlic naan", "status": "OK", "open_alerts": 0,
                          "scrap_rate_15m_pct": 0.52, "current_lot": "L1-20261019-048",
                          "as_of": "2026-10-20T05:59:00Z"}],
    "gold_dq_summary": [{"event_type": "unit", "rule": "weight_in_bounds", "records": 359}],
}

GOOD = ("**Shift A:** L1 oven zone 3 ran high 08:09-09:02, peak 17.3 °C, alert at 08:12.\n"
        "- Lots L1-20261019-005 and L1-20261019-006 were made during it; 165 of 3,172 packs rejected.\n"
        "- L1: 28,737 packs, scrap 1.16%, giveaway CAD 141.20.")
WRONG = GOOD.replace("scrap 1.16%", "scrap 1.5%")


def tool_call_turn():
    return {"role": "assistant", "content": None, "tool_calls": [
        {"id": "c1", "type": "function", "function": {"name": "get_shift_kpis",
                                                       "arguments": json.dumps({"shift_date": "2026-10-19", "shift": "A"})}},
        {"id": "c2", "type": "function", "function": {"name": "get_shift_alerts",
                                                       "arguments": json.dumps({"shift_date": "2026-10-19", "shift": "A"})}}]}


def test_verifier_accepts_rounded_tool_values_and_flags_invented_ones():
    tools = GoldTools(GOLD)
    results = [tools.get_shift_kpis("2026-10-19", "A"), tools.get_shift_alerts("2026-10-19", "A")]
    assert verify(GOOD, results).unverified == []
    bad = verify(GOOD + " Lot L1-20261019-009 at 08:15, 4.2% scrap.", results)
    assert set(bad.unverified) == {"L1-20261019-009", "08:15", "4.2"}


def test_agent_uses_tools_and_every_figure_is_verified(fake_llm):
    fake_llm["replies"] = [tool_call_turn(), {"role": "assistant", "content": GOOD}]
    [rec] = generate(GoldTools(GOLD), fake_llm["client"], [("2026-10-19", "A")], log=lambda *_: None)
    assert rec["writer"] == "llm" and rec["status"] == "verified" and rec["retried"] is False
    assert rec["figures_verified"] == rec["figures_total"] > 10
    assert [c["tool"] for c in rec["tool_calls"]] == ["get_shift_kpis", "get_shift_alerts"]
    first = fake_llm["requests"][0]
    assert first["auth"] == "Bearer test-key" and {t["function"]["name"] for t in first["body"]["tools"]} >= {"get_shift_kpis"}


def test_invented_number_triggers_one_retry_that_fixes_it(fake_llm):
    fake_llm["replies"] = [tool_call_turn(), {"role": "assistant", "content": WRONG},
                           {"role": "assistant", "content": GOOD}]
    [rec] = generate(GoldTools(GOLD), fake_llm["client"], [("2026-10-19", "A")], log=lambda *_: None)
    assert rec["retried"] is True and rec["status"] == "verified"
    retry_prompt = fake_llm["requests"][-1]["body"]["messages"][-1]["content"]
    assert "1.5" in retry_prompt


def test_still_wrong_after_retry_is_marked_needs_review(fake_llm):
    fake_llm["replies"] = [tool_call_turn(), {"role": "assistant", "content": WRONG},
                           {"role": "assistant", "content": WRONG}]
    [rec] = generate(GoldTools(GOLD), fake_llm["client"], [("2026-10-19", "A")], log=lambda *_: None)
    assert rec["status"] == "needs_review" and rec["unverified"] == ["1.5"]


def test_api_failure_falls_back_to_template(fake_llm):
    fake_llm["status"] = 401
    [rec] = generate(GoldTools(GOLD), fake_llm["client"], [("2026-10-19", "A")], log=lambda *_: None)
    assert rec["writer"] == "template" and rec["llm_error"] and rec["status"] == "verified"


def test_comma_lists_are_not_read_as_thousands():
    tools = GoldTools(GOLD)
    results = [tools.get_shift_alerts("2026-10-19", "A")]
    check = verify("Check lots 005, 006. 3,172 packs.", results)
    assert check.figures == ["005", "006", "3,172"] and check.unverified == []


def test_template_without_key_covers_every_alert_and_line():
    rec = template_note(GoldTools(GOLD), "2026-10-19", "A")
    assert "lots 005, 006" in rec["note"] and "28,737 packs" in rec["note"]
    assert rec["verification"].unverified == []


def test_template_carries_the_investigation_agents_probable_cause():
    gold = GOLD | {"gold_investigations": [{"alert_id": "x", "diagnosis": {"root_cause": "burner_fault", "confidence": "high"}}]}
    res = template_note(GoldTools(gold), "2026-10-19", "A")
    assert "Probable cause: burner fault (high confidence)." in res["note"]
    assert res["verification"].unverified == []
