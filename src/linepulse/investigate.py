"""Incident-investigation agent: when an alert fires, find out *why*, not just *what*.

An alarm says "L2 oven zone 3 is running hot". The supervisor needs to know whether to call
maintenance for the burner, recalibrate a thermocouple, check the belt drive or leave the
oven alone. The same alarm has several possible causes, and only the other signals tell
them apart, so the agent gathers evidence before it decides:

    tools     -> read-only views of gold: the alert, the signal around it, every other signal
                 on the line, what came off the line (packs, rejects), related alerts
    agent     -> LLM with tool calling writes a structured diagnosis: root cause, confidence,
                 evidence, causes ruled out, recommended actions
    verify    -> every figure in the diagnosis must appear in a tool result (handover.verify)
    rules     -> a deterministic investigator using the same tools: the fallback when no LLM
                 is configured, and a second opinion to compare against
    lookup    -> the naive baseline an alarm system gives you: one cause per signal

Every run is recorded step by step (what was checked, why, what came back), so the dashboard
can replay the investigation and a person can accept or override the diagnosis.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

from . import config as C
from .handover import ChatClient, LLMError, Verification, verify

CAUSES = ("burner_fault", "thermocouple_drift", "belt_slowdown", "filler_drift")
CAUSE_LABELS = {"burner_fault": "Burner fault", "thermocouple_drift": "Thermocouple drift",
                "belt_slowdown": "Belt slowdown", "filler_drift": "Filler drift", "unclear": "Unclear"}
SIGNALS = {"oven_z1_c": ("Oven zone 1", "°C", 1), "oven_z2_c": ("Oven zone 2", "°C", 1),
           "oven_z3_c": ("Oven zone 3", "°C", 1), "belt_speed_mpm": ("Belt speed", "m/min", 2),
           "proofer_rh_pct": ("Proofer humidity", "%", 1), "fill_weight_g": ("Fill weight", "g", 2)}
LOOKUP = {"oven_z1_c": "burner_fault", "oven_z2_c": "burner_fault", "oven_z3_c": "burner_fault",
          "belt_speed_mpm": "belt_slowdown", "fill_weight_g": "filler_drift"}
BEFORE_MIN = 60   # length of the baseline window
GAP_MIN = 20      # baseline ends this long before the alert: slow faults start well before they alarm


def _ts(s) -> pd.Timestamp:
    return pd.Timestamp(s).tz_localize(None) if pd.Timestamp(s).tzinfo else pd.Timestamp(s)


def _hm(t) -> str:
    return _ts(t).strftime("%H:%M")


def _iso(t) -> str:
    return _ts(t).strftime("%Y-%m-%dT%H:%M:%SZ")


def _r(x, nd):
    return None if x is None or pd.isna(x) else round(float(x), nd)


# ---------------------------------------------------------------- tools over gold data

class IncidentTools:
    """Read-only evidence tools. Each returns plain JSON-able data with values pre-rounded."""

    def __init__(self, gold: dict[str, list[dict]]):
        self.alerts = pd.DataFrame(gold["gold_alerts"])
        sm = pd.DataFrame(gold["gold_signal_minute"])
        sm["minute"] = pd.to_datetime(sm["minute"]).dt.tz_localize(None)
        self.sm = {k: g.sort_values("minute").set_index("minute") for k, g in sm.groupby(["line_id", "signal"])}
        q = pd.DataFrame(gold["gold_quality_minute"])
        q["minute"] = pd.to_datetime(q["minute"]).dt.tz_localize(None)
        self.q = {k: g.sort_values("minute").set_index("minute") for k, g in q.groupby("line_id")}

    @classmethod
    def from_export(cls, export_dir) -> "IncidentTools":
        from pathlib import Path
        d = Path(export_dir)
        return cls({n: json.loads((d / f"{n}.json").read_text())
                    for n in ("gold_alerts", "gold_signal_minute", "gold_quality_minute")})

    SPECS = [
        {"name": "get_alert", "description": "The alert being investigated: line, signal, direction, when, how far off, lots made.",
         "parameters": {"type": "object", "properties": {"alert_id": {"type": "string"}}, "required": ["alert_id"]}},
        {"name": "get_signal_window",
         "description": "One signal around the alert: deviation from setpoint before and during, whether it changed "
                        "suddenly or gradually, reading noise during vs before (noise_ratio), 5-minute samples.",
         "parameters": {"type": "object", "properties": {
             "line_id": {"type": "string"}, "signal": {"type": "string", "enum": list(SIGNALS)},
             "start": {"type": "string", "description": "ISO time, e.g. the alert's window_start"},
             "end": {"type": "string", "description": "ISO time, e.g. the alert's window_end"}},
             "required": ["line_id", "signal", "start", "end"]}},
        {"name": "compare_line_signals",
         "description": "Every signal on the line (three oven zones, belt speed, proofer humidity, fill weight): mean "
                        "deviation from setpoint in a baseline hour before vs during the window, the change, and noise_ratio.",
         "parameters": {"type": "object", "properties": {
             "line_id": {"type": "string"}, "start": {"type": "string"}, "end": {"type": "string"}},
             "required": ["line_id", "start", "end"]}},
        {"name": "get_product_quality",
         "description": "What came off the line in a baseline hour before vs during the window: packs per minute, bake-defect "
                        "and underweight rejects per 1,000 packs, mean pack weight, giveaway per pack.",
         "parameters": {"type": "object", "properties": {
             "line_id": {"type": "string"}, "start": {"type": "string"}, "end": {"type": "string"}},
             "required": ["line_id", "start", "end"]}},
        {"name": "get_related_alerts",
         "description": "Other alerts on the same line near this one, alerts on other lines at the same time "
                        "(a shared cause such as gas supply?), and earlier alerts on the same signal.",
         "parameters": {"type": "object", "properties": {"alert_id": {"type": "string"}}, "required": ["alert_id"]}},
    ]

    def call(self, name: str, args: dict) -> object:
        fn = {"get_alert": self.get_alert, "get_signal_window": self.get_signal_window,
              "compare_line_signals": self.compare_line_signals, "get_product_quality": self.get_product_quality,
              "get_related_alerts": self.get_related_alerts}.get(name)
        if fn is None:
            return {"error": f"unknown tool {name}"}
        try:
            return fn(**args)
        except (TypeError, KeyError, ValueError, IndexError) as e:
            return {"error": f"bad arguments for {name}: {e}"}

    # -- helpers
    def _alert(self, alert_id: str) -> pd.Series:
        m = self.alerts[self.alerts.alert_id == alert_id]
        if m.empty:
            raise KeyError(f"no alert {alert_id}")
        return m.iloc[0]

    def _windows(self, start, end):
        s, e = _ts(start).floor("min"), _ts(end).floor("min")
        b_end = s - timedelta(minutes=GAP_MIN)
        return (b_end - timedelta(minutes=BEFORE_MIN), b_end), (s, e)

    @staticmethod
    def _slice(df: pd.DataFrame, w) -> pd.DataFrame:
        return df[(df.index >= w[0]) & (df.index < w[1])]

    # -- tools
    def get_alert(self, alert_id: str) -> dict:
        a = self._alert(alert_id)
        name, unit, nd = SIGNALS[a.signal]
        lots = list(a.affected_lots)
        return {"alert_id": alert_id, "line_id": a.line_id, "signal": a.signal, "signal_name": name, "unit": unit,
                "direction": a.direction, "status": a.status, "started": _hm(a.start_minute),
                "detected": _hm(a.detected_at), "ended": _hm(a.end_minute), "duration_min": int(a.duration_min),
                "peak_deviation": _r(a.peak_abs_dev, nd), "alarm_threshold": _r(a.threshold, nd),
                "window_start": _iso(a.start_minute), "window_end": _iso(a.end_minute),
                "affected_lots": lots, "lot_numbers": [x.rsplit("-", 1)[-1] for x in lots],
                "packs_in_window": int(a.units_in_window), "rejects_in_window": int(a.rejects_in_window)}

    def get_signal_window(self, line_id: str, signal: str, start: str, end: str) -> dict:
        name, unit, nd = SIGNALS[signal]
        df = self.sm[(line_id, signal)]
        before_w, during_w = self._windows(start, end)
        before, during = self._slice(df, before_w), self._slice(df, during_w)
        if during.empty:
            return {"error": "no data in that window"}
        dev = during.deviation
        peak_i = dev.abs().idxmax()
        peak = float(dev.loc[peak_i])
        # Onset: minutes from first reaching 20% of the peak deviation to first reaching 80% of it.
        look = self._slice(df, (before_w[1], during_w[1])).deviation * (1 if peak > 0 else -1)
        t20 = look[look >= 0.2 * abs(peak)].index.min()
        t80 = look[look >= 0.8 * abs(peak)].index.min()
        rise = int((t80 - t20).total_seconds() // 60) if pd.notna(t20) and pd.notna(t80) else None
        noise_before = before.sd_value.mean() if not before.empty else None
        noise_during = during.sd_value.mean()
        samples = self._slice(df, (during_w[0] - timedelta(minutes=15), during_w[1] + timedelta(minutes=6)))
        samples = samples[samples.index.minute % 5 == 0]
        return {"line_id": line_id, "signal": signal, "signal_name": name, "unit": unit,
                "setpoint": _r(df.setpoint.iloc[0], nd), "window": f"{_hm(during_w[0])}-{_hm(during_w[1])}", "baseline": f"{_hm(before_w[0])}-{_hm(before_w[1])}",
                "mean_deviation_before": _r(before.deviation.mean(), nd) if not before.empty else None,
                "mean_deviation_during": _r(dev.mean(), nd), "peak_deviation": _r(peak, nd),
                "peak_at": _hm(peak_i), "onset": None if rise is None else ("sudden" if rise <= 3 else "gradual"),
                "minutes_to_reach_peak": rise,
                "reading_noise_before": _r(noise_before, 3), "reading_noise_during": _r(noise_during, 3),
                "noise_ratio": _r(noise_during / noise_before, 1) if noise_before else None,
                "samples": [{"time": _hm(t), "deviation": _r(v, nd)} for t, v in samples.deviation.items()]}

    def compare_line_signals(self, line_id: str, start: str, end: str) -> dict:
        before_w, during_w = self._windows(start, end)
        out = []
        for sig, (name, unit, nd) in SIGNALS.items():
            df = self.sm.get((line_id, sig))
            if df is None:
                continue
            b, d = self._slice(df, before_w), self._slice(df, during_w)
            if d.empty:
                continue
            mb = b.deviation.mean() if not b.empty else 0.0
            out.append({"signal": sig, "signal_name": name, "unit": unit,
                        "mean_deviation_before": _r(mb, nd), "mean_deviation_during": _r(d.deviation.mean(), nd),
                        "change": _r(d.deviation.mean() - mb, nd),
                        "noise_ratio": _r(d.sd_value.mean() / b.sd_value.mean(), 1) if not b.empty else None,
                        "alarm_threshold": _r(df.threshold.iloc[0], nd)})
        return {"line_id": line_id, "window": f"{_hm(during_w[0])}-{_hm(during_w[1])}",
                "baseline": f"{_hm(before_w[0])}-{_hm(before_w[1])}", "signals": out}

    def get_product_quality(self, line_id: str, start: str, end: str) -> dict:
        before_w, during_w = self._windows(start, end)
        q = self.q[line_id]

        def summary(df):
            packs = int(df.units.sum())
            if not packs:
                return None
            return {"minutes": int(len(df)), "packs": packs, "packs_per_min": _r(packs / len(df), 1),
                    "bake_defects": int(df.rejects_bake_defect.sum()),
                    "bake_defects_per_1000": _r(1000 * df.rejects_bake_defect.sum() / packs, 1),
                    "underweight": int(df.rejects_underweight.sum()),
                    "underweight_per_1000": _r(1000 * df.rejects_underweight.sum() / packs, 1),
                    "mean_pack_weight_g": _r((df.mean_weight_g * df.units).sum() / packs, 2),
                    "giveaway_g_per_pack": _r(df.giveaway_g_per_pack.mean(), 2)}

        b, d = summary(self._slice(q, before_w)), summary(self._slice(q, during_w))
        out = {"line_id": line_id, "window": f"{_hm(during_w[0])}-{_hm(during_w[1])}",
               "baseline": f"{_hm(before_w[0])}-{_hm(before_w[1])}", "per": 1000, "label_weight_g": C.LABEL_WEIGHT_G, "target_weight_g": C.TARGET_WEIGHT_G,
               "before": b, "during": d}
        if b and d:
            out["packs_per_min_change_pct"] = _r(100 * (d["packs_per_min"] - b["packs_per_min"]) / b["packs_per_min"], 0)
            out["bake_defect_rate_ratio"] = (_r(d["bake_defects_per_1000"] / b["bake_defects_per_1000"], 1)
                                             if b["bake_defects_per_1000"] else None)
        return out

    def get_related_alerts(self, alert_id: str) -> dict:
        a = self._alert(alert_id)
        s, e = _ts(a.start_minute), _ts(a.end_minute)
        al = self.alerts.assign(ts_s=self.alerts.start_minute.map(_ts), ts_e=self.alerts.end_minute.map(_ts))
        near = (al.ts_s <= e + timedelta(minutes=30)) & (al.ts_e >= s - timedelta(minutes=30)) & (al.alert_id != alert_id)

        def brief(r):
            name, unit, nd = SIGNALS[r.signal]
            return {"alert_id": r.alert_id, "line_id": r.line_id, "signal_name": name, "direction": r.direction,
                    "started": _hm(r.ts_s), "ended": _hm(r.ts_e), "peak_deviation": _r(r.peak_abs_dev, nd), "unit": unit}

        same_line = al[near & (al.line_id == a.line_id)]
        other = al[near & (al.line_id != a.line_id)]
        earlier = al[(al.line_id == a.line_id) & (al.signal == a.signal) & (al.ts_e < s)]
        return {"same_line_nearby": [brief(r) for r in same_line.itertuples()],
                "other_lines_same_time": [brief(r) for r in other.itertuples()],
                "earlier_same_signal": [brief(r) for r in earlier.itertuples()]}


# ---------------------------------------------------------------- diagnosis checks

def diagnosis_text(d: dict) -> str:
    parts = [d.get("summary", "")] + list(d.get("evidence", []))
    parts += [r.get("why", "") for r in d.get("ruled_out", []) if isinstance(r, dict)]
    parts += list(d.get("actions", []))
    return "\n".join(str(p) for p in parts)


def check_diagnosis(d: dict, results: list) -> tuple[list[str], Verification]:
    """Structural problems (bad cause, missing fields) and figure verification."""
    problems = []
    if d.get("root_cause") not in (*CAUSES, "unclear"):
        problems.append(f"root_cause must be one of {', '.join(CAUSES)} or unclear")
    if d.get("confidence") not in ("high", "medium", "low"):
        problems.append("confidence must be high, medium or low")
    for k in ("summary", "evidence", "actions"):
        if not d.get(k):
            problems.append(f"{k} is missing")
    return problems, verify(diagnosis_text(d), results)


def parse_json(text: str) -> dict | None:
    m = re.search(r"\{.*\}", text or "", re.S)
    if not m:
        return None
    try:
        out = json.loads(m.group(0))
    except json.JSONDecodeError:
        return None
    return out if isinstance(out, dict) else None


# ---------------------------------------------------------------- LLM agent

SYSTEM_PROMPT = """You investigate drift alerts on a bakery line (two naan lines, a three-zone tunnel oven,
a belt, a proofer, a filler and a checkweigher) and decide the most likely root cause.

Possible root causes:
- burner_fault: the burner or gas valve in one oven zone is putting in too much or too little heat.
- thermocouple_drift: the temperature sensor in one zone is reading wrong; the oven itself is fine.
- belt_slowdown: the oven belt is running slower than setpoint.
- filler_drift: the filler is putting too much or too little dough in each pack.
- unclear: the evidence does not support any of these.

Troubleshooting guide (plant knowledge, not rules about this data):
- A real oven temperature change shows up in the product: bake defects rise. A sensor problem
  does not change the product, and a failing thermocouple often reads noisier than usual.
- If the belt slows, product spends longer in the oven: bake defects rise and packs per minute
  fall. With less dough absorbing heat, several zones can read hotter at once.
- Filler problems move pack weight: heavy packs mean giveaway, light packs mean underweight rejects.
- Look at the other signals on the line and at the product before deciding. Do not decide from
  the alarm's signal alone.

How to work:
- Call tools to gather evidence. Several tools can be called in one turn.
- Before each tool call, write one short sentence saying what you are checking and why.
- Every number, clock time and lot ID you write must be copied from a tool result. Do not compute
  new numbers; use the ratios and changes the tools provide.
- When done, reply with only a JSON object, no markdown:
  {"root_cause": "...", "confidence": "high|medium|low", "summary": "one sentence",
   "evidence": ["2-4 short facts with figures"], "ruled_out": [{"cause": "...", "why": "..."}],
   "actions": ["2-3 concrete actions for the supervisor"]}"""


def run_agent(client: ChatClient, tools: IncidentTools, alert_id: str, max_rounds: int = 8) -> dict:
    messages = [{"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Investigate alert {alert_id}."}]
    steps, results = [], []
    retried = False
    while True:
        for _ in range(max_rounds):
            msg = client.complete(messages, IncidentTools.SPECS)
            tool_calls = msg.get("tool_calls") or []
            thought = (msg.get("content") or "").strip()
            messages.append({"role": "assistant", "content": (thought or None) if tool_calls else thought,
                             **({"tool_calls": tool_calls} if tool_calls else {})})
            if not tool_calls:
                break
            for i, tc in enumerate(tool_calls):
                name = tc["function"]["name"]
                try:
                    args = json.loads(tc["function"].get("arguments") or "{}")
                except json.JSONDecodeError:
                    args = {}
                t0 = time.time()
                out = tools.call(name, args)
                steps.append({"thought": thought if i == 0 else "", "tool": name, "args": args, "result": out,
                              "ms": round(1000 * (time.time() - t0), 1)})
                results.append(out)
                messages.append({"role": "tool", "tool_call_id": tc.get("id", name), "content": json.dumps(out)})
        else:
            raise LLMError("agent did not finish within the tool-call budget")
        diagnosis = parse_json(messages[-1]["content"])
        if diagnosis is None:
            problems, check = ["reply was not a JSON object"], Verification()
        else:
            problems, check = check_diagnosis(diagnosis, results)
        if (problems or check.unverified) and not retried:
            retried = True
            fix = problems + ([f"these figures are not in any tool result: {', '.join(check.unverified)}"]
                              if check.unverified else [])
            messages.append({"role": "user", "content": "Fix your diagnosis: " + "; ".join(fix) +
                             ". Reply with only the corrected JSON object, using values copied from tool results."})
            continue
        if diagnosis is None:
            raise LLMError("model did not return a JSON diagnosis")
        return {"steps": steps, "diagnosis": diagnosis, "verification": check, "problems": problems, "retried": retried}


# ---------------------------------------------------------------- deterministic investigator

ACTIONS = {
    "burner_fault": ["Ask maintenance to check the {zone} burner and gas valve before the next lot.",
                     "Hold and inspect {lots} for bake quality."],
    "thermocouple_drift": ["Check {zone} with a handheld probe, then recalibrate or replace the thermocouple.",
                           "Do not adjust the burner: the oven is at temperature, only the reading is off."],
    "belt_slowdown": ["Check the oven belt drive (motor, VFD, chain tension) and restore setpoint speed.",
                      "Hold and inspect {lots} for over-bake."],
    "filler_drift": ["Re-zero the checkweigher and recalibrate the filler to target weight.",
                     "{weight_action}"],
    "unclear": ["Ask the line operator to inspect the {signal} locally.", "Keep the alert open and recheck in 15 minutes."],
}


def _lots(alert: dict) -> str:
    n = alert["lot_numbers"]
    return ("lot " if len(n) == 1 else "lots ") + ", ".join(n) if n else "the current lot"


def rules_investigation(tools: IncidentTools, alert_id: str) -> dict:
    """Same tools, fixed reasoning. Thoughts and diagnosis only quote values the tools returned."""
    steps, results = [], []

    def use(thought, name, **args):
        out = tools.call(name, args)
        steps.append({"thought": thought, "tool": name, "args": args, "result": out, "ms": 0.0})
        results.append(out)
        return out

    a = use("Start from the alert itself: which signal, how far off, and for how long.", "get_alert", alert_id=alert_id)
    line, sig, unit = a["line_id"], a["signal"], a["unit"]
    w = dict(start=a["window_start"], end=a["window_end"])
    s = use(f"Look at {a['signal_name'].lower()} around the alert: did it jump or creep, and is the reading noisier?",
            "get_signal_window", line_id=line, signal=sig, **w)
    c = use(f"Compare every other signal on {line}: is this one zone, or is something upstream moving too?",
            "compare_line_signals", line_id=line, **w)
    q = use("Check the product: a real process change shows up in rejects and throughput, a sensor fault does not.",
            "get_product_quality", line_id=line, **w)
    r = use("Look for related alerts on this line, on the other line, and earlier on the same signal.",
            "get_related_alerts", alert_id=alert_id)

    by = {x["signal"]: x for x in c["signals"]}
    belt = by.get("belt_speed_mpm", {})
    before, during = q.get("before") or {}, q.get("during") or {}
    ratio = q.get("bake_defect_rate_ratio")
    noise = s.get("noise_ratio")
    belt_change = belt.get("change") or 0.0
    defects_up = (ratio is not None and ratio >= 2.0
                  and during.get("bake_defects_per_1000", 0) - before.get("bake_defects_per_1000", 0) >= 8)
    other_zones_up = [x for k, x in by.items() if k.startswith("oven_") and k != sig and (x["change"] or 0) >= 2.5]

    evidence, ruled_out = [], []
    if belt_change <= -0.3:
        cause = "belt_slowdown"
        evidence.append(f"Belt speed changed by {belt_change} m/min against the baseline hour.")
        if q.get("packs_per_min_change_pct") is not None:
            evidence.append(f"Packs per minute went from {before['packs_per_min']} to {during['packs_per_min']} "
                            f"({q['packs_per_min_change_pct']}%).")
        if defects_up:
            evidence.append(f"Bake defects went from {before['bake_defects_per_1000']} to "
                            f"{during['bake_defects_per_1000']} per 1,000 packs.")
        if other_zones_up and sig.startswith("oven_"):
            evidence.append("Other zones also read hotter: " + ", ".join(
                f"{x['signal_name'].lower()} {x['change']:+} °C" for x in other_zones_up) + ".")
        if sig.startswith("oven_"):
            ruled_out.append({"cause": "burner_fault", "why": "The whole oven moved with the belt, not one zone."})
            ruled_out.append({"cause": "thermocouple_drift", "why": "The product changed, so the oven really changed."})
        confidence = "high" if q.get("packs_per_min_change_pct", 0) <= -8 else "medium"
    elif sig == "fill_weight_g":
        cause = "filler_drift"
        evidence.append(f"Mean pack weight was {during.get('mean_pack_weight_g')} g against a "
                        f"{q['target_weight_g']} g target ({before.get('mean_pack_weight_g')} g in the baseline hour).")
        if a["direction"] == "high":
            evidence.append(f"Giveaway was {during.get('giveaway_g_per_pack')} g per pack over the "
                            f"{q['label_weight_g']} g label.")
        else:
            evidence.append(f"Underweight rejects went from {before.get('underweight_per_1000')} to "
                            f"{during.get('underweight_per_1000')} per 1,000 packs.")
        evidence.append(f"Belt speed changed by {belt_change} m/min, so the line rate is normal.")
        ruled_out.append({"cause": "belt_slowdown", "why": "Belt speed held at setpoint."})
        confidence = "high"
    elif sig.startswith("oven_") and defects_up:
        cause = "burner_fault"
        evidence.append(f"{a['signal_name']} peaked {s['peak_deviation']} °C off setpoint ({s['onset']} onset).")
        evidence.append(f"Bake defects went from {before['bake_defects_per_1000']} to "
                        f"{during['bake_defects_per_1000']} per 1,000 packs ({ratio}x).")
        evidence.append(f"Belt speed changed by {belt_change} m/min.")
        ruled_out.append({"cause": "thermocouple_drift", "why": "Product quality changed, so the oven temperature really changed."})
        ruled_out.append({"cause": "belt_slowdown", "why": "Belt speed held at setpoint."})
        confidence = "high" if not other_zones_up else "medium"
    elif sig.startswith("oven_"):
        cause = "thermocouple_drift"
        evidence.append(f"{a['signal_name']} read {s['peak_deviation']} °C off setpoint ({s['onset']} onset).")
        evidence.append(f"Bake defects stayed at {during.get('bake_defects_per_1000')} per 1,000 packs "
                        f"({before.get('bake_defects_per_1000')} in the baseline hour): the product did not change.")
        if noise is not None:
            evidence.append(f"The reading was {noise}x noisier than in the baseline hour.")
        evidence.append(f"Belt speed changed by {belt_change} m/min.")
        ruled_out.append({"cause": "burner_fault", "why": "A real heat change would have raised bake defects."})
        ruled_out.append({"cause": "belt_slowdown", "why": "Belt speed held at setpoint."})
        confidence = "high" if (noise or 0) >= 1.5 else "medium"
    else:
        cause, confidence = "unclear", "low"
        evidence.append(f"{a['signal_name']} peaked {s['peak_deviation']} {unit} off setpoint.")

    zone = a["signal_name"].lower()
    weight_action = ("Hold and re-check underweight packs from " + _lots(a) + "." if a["direction"] == "low"
                     else "Recover giveaway by trimming fill toward target; no product hold needed.")
    actions = [t.format(zone=zone, lots=_lots(a), signal=zone, weight_action=weight_action) for t in ACTIONS[cause]]
    others = r["other_lines_same_time"]
    if others:
        evidence.append("Another line alarmed at the same time (" + ", ".join(
            f"{o['line_id']} {o['signal_name'].lower()} from {o['started']}" for o in others) +
            "); investigate it separately before assuming a shared cause.")
    summary = {
        "belt_slowdown": f"{line}'s belt slowed, so product baked longer and the oven read hot: a belt drive problem, not the burners.",
        "filler_drift": f"{line}'s filler drifted {a['direction']}: pack weight moved while the rest of the line held steady.",
        "burner_fault": f"{line} {zone} really ran {a['direction']}: bake defects rose while the belt held speed.",
        "thermocouple_drift": f"{line} {zone}'s thermocouple is reading wrong: the product is unchanged, so the oven is fine.",
        "unclear": "The evidence does not point to one cause.",
    }[cause]
    diagnosis = {"root_cause": cause, "confidence": confidence, "summary": summary, "evidence": evidence,
                 "ruled_out": ruled_out, "actions": actions}
    problems, check = check_diagnosis(diagnosis, results)
    return {"steps": steps, "diagnosis": diagnosis, "verification": check, "problems": problems, "retried": False}


# ---------------------------------------------------------------- ground truth and entry point

GRACE_MIN = 15


def match_truth(alert: dict, truth: dict | None) -> dict | None:
    """The injected fault an alert belongs to (same line, a signal it moves, overlapping in time)."""
    if not truth:
        return None
    origin = datetime.fromisoformat(truth["origin"])
    s, e = _ts(alert["start_minute"]).to_pydatetime(), _ts(alert["end_minute"]).to_pydatetime()
    for f in truth["faults"]:
        onset = origin + timedelta(minutes=f["start_min"])
        end = origin + timedelta(minutes=f["end_min"] + GRACE_MIN)
        if f["line_id"] == alert["line_id"] and alert["signal"] in (f.get("signals") or [f["signal"]]) \
                and e >= onset and s <= end:
            return {"fault_id": f["fault_id"], "cause": f.get("cause")}
    return None


def investigate(tools: IncidentTools, alert_id: str, client: ChatClient | None, truth: dict | None = None,
                log=print) -> dict:
    writer, model, error = "rules", None, None
    t0 = time.time()
    res = None
    if client is not None:
        try:
            res = run_agent(client, tools, alert_id)
            writer, model = "llm", client.model
        except LLMError as e:
            error = str(e)
            log(f"LLM failed for {alert_id}, using rules: {e}")
    if res is None:
        res = rules_investigation(tools, alert_id)
    alert = tools.alerts[tools.alerts.alert_id == alert_id].iloc[0].to_dict()
    v: Verification = res["verification"]
    rec = {"alert_id": alert_id, "line_id": alert["line_id"], "signal": alert["signal"],
           "start_minute": _iso(alert["start_minute"]), "writer": writer, "model": model,
           "steps": res["steps"], "diagnosis": res["diagnosis"],
           "figures_total": v.total, "figures_verified": v.verified, "unverified": v.unverified,
           "status": "verified" if not v.unverified and not res["problems"] else "needs_review",
           "problems": res["problems"], "retried": res["retried"], "seconds": round(time.time() - t0, 1),
           "llm_error": error, "baseline_lookup": LOOKUP.get(alert["signal"], "unclear"),
           "ground_truth": match_truth(alert, truth)}
    log(f"{alert_id}: {writer} -> {res['diagnosis']['root_cause']} ({res['diagnosis']['confidence']}), "
        f"{v.verified}/{v.total} figures verified"
        + (f", truth {rec['ground_truth']['cause']}" if rec["ground_truth"] else ""))
    return rec


@dataclass
class Score:
    n: int = 0
    correct: int = 0

    def add(self, ok: bool):
        self.n += 1
        self.correct += int(ok)

    @property
    def rate(self):
        return round(self.correct / self.n, 3) if self.n else None
