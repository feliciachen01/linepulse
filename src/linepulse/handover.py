"""Shift-handover agent: an LLM writes the end-of-shift note, but may only use numbers it
fetched through tools over the gold tables, and every figure it writes is checked.

    tools  -> read-only views of gold tables (KPIs, alerts, line status, data quality)
    agent  -> OpenAI-compatible chat API with tool calling (Gemini free tier by default)
    verify -> every number, time and lot ID in the note must appear in a tool result
    retry  -> one retry listing the unverified figures; still wrong -> "needs_review"
    fallback -> no API key: a deterministic template writes the note from the same tools

Standard library only, so it runs anywhere: CI, a laptop, a Codespace.
"""

from __future__ import annotations

import json
import os
import re
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

SHIFT_HOURS = {"A": (6, 14), "B": (14, 22), "C": (22, 6)}
SIGNAL_NAMES = {"oven_z1_c": ("oven zone 1", "°C"), "oven_z2_c": ("oven zone 2", "°C"),
                "oven_z3_c": ("oven zone 3", "°C"), "fill_weight_g": ("fill weight", "g"),
                "belt_speed_mpm": ("belt speed", "m/min"), "proofer_rh_pct": ("proofer humidity", "%")}

DEFAULT_BASE_URL = "https://generativelanguage.googleapis.com/v1beta/openai"
DEFAULT_MODEL = "gemini-2.5-flash"


# ---------------------------------------------------------------- tools over gold data

def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)


def _hm(s: str) -> str:
    return _ts(s).strftime("%H:%M")


def shift_window(shift_date: str, shift: str) -> tuple[datetime, datetime]:
    start_h, end_h = SHIFT_HOURS[shift]
    d = datetime.fromisoformat(shift_date)
    start = d + timedelta(hours=start_h)
    end = d + timedelta(hours=end_h if end_h > start_h else 24 + end_h)
    return start, end


class GoldTools:
    """Read-only tools the agent may call. Each returns plain JSON-able data."""

    def __init__(self, gold: dict[str, list[dict]]):
        self.gold = gold
        # Diagnoses from the investigation agent, if it has run: alert_id -> diagnosis.
        self.diagnoses = {r["alert_id"]: r["diagnosis"] for r in gold.get("gold_investigations", [])}

    @classmethod
    def from_export(cls, export_dir: str | Path) -> "GoldTools":
        d = Path(export_dir)
        names = ["gold_shift_kpis", "gold_alerts", "gold_line_status", "gold_dq_summary"]
        gold = {n: json.loads((d / f"{n}.json").read_text()) for n in names}
        if (d / "gold_investigations.json").exists():
            gold["gold_investigations"] = json.loads((d / "gold_investigations.json").read_text())
        return cls(gold)

    SPECS = [
        {"name": "list_shifts", "description": "List the shifts that have data, oldest first.",
         "parameters": {"type": "object", "properties": {}}},
        {"name": "get_shift_kpis",
         "description": "Per-line KPIs for one shift (packs, scrap %, rejects by reason, giveaway) plus both-line totals.",
         "parameters": {"type": "object", "properties": {
             "shift_date": {"type": "string", "description": "YYYY-MM-DD, the date the shift started"},
             "shift": {"type": "string", "enum": ["A", "B", "C"]}}, "required": ["shift_date", "shift"]}},
        {"name": "get_shift_alerts",
         "description": "Drift alerts that started during one shift: what drifted, when, how far, lots made during it, "
                        "and the probable cause if the investigation agent has diagnosed it.",
         "parameters": {"type": "object", "properties": {
             "shift_date": {"type": "string"}, "shift": {"type": "string", "enum": ["A", "B", "C"]}},
             "required": ["shift_date", "shift"]}},
        {"name": "get_line_status", "description": "Latest status of each line (open alerts, 15-minute scrap, current lot).",
         "parameters": {"type": "object", "properties": {}}},
        {"name": "get_data_quality", "description": "Records quarantined by data-quality rule (whole run).",
         "parameters": {"type": "object", "properties": {}}},
    ]

    def call(self, name: str, args: dict) -> object:
        fn = {"list_shifts": self.list_shifts, "get_shift_kpis": self.get_shift_kpis,
              "get_shift_alerts": self.get_shift_alerts, "get_line_status": self.get_line_status,
              "get_data_quality": self.get_data_quality}.get(name)
        if fn is None:
            return {"error": f"unknown tool {name}"}
        try:
            return fn(**args)
        except (TypeError, KeyError, ValueError) as e:
            return {"error": f"bad arguments for {name}: {e}"}

    def list_shifts(self) -> list[dict]:
        seen = sorted({(r["shift_date"], r["shift"]) for r in self.gold["gold_shift_kpis"]},
                      key=lambda x: shift_window(*x)[0])
        return [{"shift_date": d, "shift": s, "hours": f"{SHIFT_HOURS[s][0]:02d}:00-{SHIFT_HOURS[s][1]:02d}:00"}
                for d, s in seen]

    def get_shift_kpis(self, shift_date: str, shift: str) -> dict:
        rows = [r for r in self.gold["gold_shift_kpis"] if r["shift_date"] == shift_date and r["shift"] == shift]
        lines = [{
            "line_id": r["line_id"], "packs": r["units"], "rejects": r["rejects"],
            "scrap_rate_pct": round(r["scrap_rate_pct"], 2),
            "rejects_underweight": r["rejects_underweight"], "rejects_bake_defect": r["rejects_bake_defect"],
            "avg_accepted_weight_g": r["avg_accepted_weight_g"], "giveaway_g_per_pack": round(r["giveaway_g_per_pack"], 2),
            "giveaway_kg": r["giveaway_kg"], "giveaway_cost_cad": r["giveaway_cost_cad"], "alerts": r["alerts"],
        } for r in sorted(rows, key=lambda r: r["line_id"])]
        packs = sum(r["packs"] for r in lines)
        rejects = sum(r["rejects"] for r in lines)
        totals = {"packs": packs, "rejects": rejects,
                  "scrap_rate_pct": round(100 * rejects / packs, 2) if packs else 0.0,
                  "giveaway_kg": round(sum(r["giveaway_kg"] for r in lines), 1),
                  "giveaway_cost_cad": round(sum(r["giveaway_cost_cad"] for r in lines), 2),
                  "alerts": sum(r["alerts"] for r in lines)}
        return {"shift_date": shift_date, "shift": shift, "lines": lines, "totals": totals}

    def get_shift_alerts(self, shift_date: str, shift: str) -> list[dict]:
        start, end = shift_window(shift_date, shift)
        out = []
        for a in sorted(self.gold["gold_alerts"], key=lambda a: a["start_minute"]):
            if not (start <= _ts(a["start_minute"]) < end):
                continue
            name, unit = SIGNAL_NAMES.get(a["signal"], (a["signal"], ""))
            out.append({
                "line_id": a["line_id"], "signal": name, "unit": unit, "direction": a["direction"],
                "started": _hm(a["start_minute"]), "detected": _hm(a["detected_at"]), "ended": _hm(a["end_minute"]),
                "duration_min": a["duration_min"], "peak_deviation": round(a["peak_abs_dev"], 1),
                "affected_lots": a["affected_lots"],
                "lot_numbers": [lot.rsplit("-", 1)[-1] for lot in a["affected_lots"]],
                "packs_in_window": a["units_in_window"],
                "rejects_in_window": a["rejects_in_window"], "status": a["status"],
                **self._cause(a["alert_id"]),
            })
        return out

    def _cause(self, alert_id: str) -> dict:
        d = self.diagnoses.get(alert_id)
        if not d:
            return {}
        return {"probable_cause": d["root_cause"].replace("_", " "), "cause_confidence": d["confidence"]}

    def get_line_status(self) -> list[dict]:
        return [{"line_id": r["line_id"], "product": r["product"], "status": r["status"],
                 "open_alerts": r["open_alerts"], "scrap_rate_15m_pct": r["scrap_rate_15m_pct"],
                 "current_lot": r["current_lot"], "as_of": _hm(r["as_of"])} for r in self.gold["gold_line_status"]]

    def get_data_quality(self) -> list[dict]:
        return [{"rule": r["rule"], "event_type": r["event_type"], "records": r["records"]}
                for r in self.gold["gold_dq_summary"]]


# ---------------------------------------------------------------- verification

_LOT = re.compile(r"\bL\d+-\d{8}-\d{3}\b")
_DATE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
_TIME = re.compile(r"\b\d{1,2}:\d{2}\b")
_IDENT = re.compile(r"\b(?:L\d+|F\d+|zone \d|zones? \d(?:\s*(?:and|,|&)\s*\d)*|Shift [ABC]|shift [ABC])\b")
_NUM = re.compile(r"(?<![\w.])[-+]?(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?")  # 28,737 yes; "005, 006" no


def _facts(value, nums: set, strings: set) -> None:
    if isinstance(value, bool):
        return
    if isinstance(value, (int, float)):
        nums.add(float(value))
    elif isinstance(value, str):
        strings.add(value)
        for t in _TIME.findall(value):
            strings.add(t)
    elif isinstance(value, dict):
        for v in value.values():
            _facts(v, nums, strings)
    elif isinstance(value, list):
        for v in value:
            _facts(v, nums, strings)


@dataclass
class Verification:
    figures: list[str] = field(default_factory=list)
    unverified: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.figures)

    @property
    def verified(self) -> int:
        return self.total - len(self.unverified)


def verify(note: str, tool_results: list) -> Verification:
    """Every lot ID, date, clock time and number in the note must come from a tool result.

    A number counts as verified if it equals a tool value after rounding to the number of
    decimals the note shows (17.27 may be written 17.3). Identifiers such as L1, zone 3 or
    Shift A are labels, not figures, and are not counted.
    """
    nums: set = set()
    strings: set = set()
    for r in tool_results:
        _facts(r, nums, strings)
    v = Verification()
    text = note
    for pattern in (_LOT, _DATE, _TIME):
        for m in pattern.findall(text):
            v.figures.append(m)
            if m not in strings and m.zfill(5) not in strings:
                v.unverified.append(m)
        text = pattern.sub(" ", text)
    text = _IDENT.sub(" ", text)
    for m in _NUM.findall(text):
        raw = m.replace(",", "").lstrip("+")
        try:
            x = float(raw)
        except ValueError:
            continue
        decimals = len(raw.split(".")[1]) if "." in raw else 0
        tol = 0.5 * 10 ** -decimals + 1e-9
        v.figures.append(m)
        if m in strings:
            continue
        if not any(abs(abs(x) - abs(f)) <= tol for f in nums):
            v.unverified.append(m)
    return v


# ---------------------------------------------------------------- LLM agent

SYSTEM_PROMPT = """You write the end-of-shift handover note for the incoming bakery line supervisor.

Use the tools to fetch facts. Rules:
- Every number, clock time, date and lot ID you write must be copied from a tool result. Do not
  add, subtract, average or estimate numbers yourself; totals are provided by get_shift_kpis.
- Do not guess causes. Say what drifted, when, how far, which lots were made during it. If an
  alert has a probable_cause from the investigation agent, give it and its confidence.
- If a shift had no alerts, say so plainly.
- Format: a one-line headline, then short bullets: Issues, Numbers, For the next shift.
  At most 130 words. Plain markdown, no tables."""


class LLMError(RuntimeError):
    pass


@dataclass
class ChatClient:
    """Minimal OpenAI-compatible /chat/completions client with tool calling."""
    base_url: str
    api_key: str
    model: str
    min_interval_s: float = 6.5      # free tiers allow ~10 requests/minute
    _last: float = 0.0

    def complete(self, messages: list, tools: list) -> dict:
        wait = self.min_interval_s - (time.time() - self._last)
        if wait > 0:
            time.sleep(wait)
        body = json.dumps({"model": self.model, "messages": messages, "temperature": 0.2,
                           "tools": [{"type": "function", "function": t} for t in tools]}).encode()
        for attempt in range(4):
            req = urllib.request.Request(f"{self.base_url.rstrip('/')}/chat/completions", data=body, method="POST",
                                         headers={"Authorization": f"Bearer {self.api_key}",
                                                  "Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(req, timeout=90) as resp:
                    self._last = time.time()
                    return json.loads(resp.read())["choices"][0]["message"]
            except urllib.error.HTTPError as e:
                self._last = time.time()
                if e.code in (429, 500, 503) and attempt < 3:
                    time.sleep(15 * (attempt + 1))
                    continue
                raise LLMError(f"HTTP {e.code}: {e.read()[:300]!r}") from e
            except urllib.error.URLError as e:
                raise LLMError(str(e)) from e
        raise LLMError("gave up after retries")


def run_agent(client: ChatClient, tools: GoldTools, shift_date: str, shift: str, max_rounds: int = 6) -> dict:
    messages = [{"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Write the handover for shift {shift} starting {shift_date}."}]
    calls, results = [], []
    retried = False
    while True:
        for _ in range(max_rounds):
            msg = client.complete(messages, GoldTools.SPECS)
            tool_calls = msg.get("tool_calls") or []
            # Tool-call turns carry null content (stricter OpenAI-compatible APIs reject "").
            messages.append({"role": "assistant", "content": (msg.get("content") or None) if tool_calls
                             else (msg.get("content") or ""), **({"tool_calls": tool_calls} if tool_calls else {})})
            if not tool_calls:
                break
            for tc in tool_calls:
                name = tc["function"]["name"]
                try:
                    args = json.loads(tc["function"].get("arguments") or "{}")
                except json.JSONDecodeError:
                    args = {}
                t0 = time.time()
                out = tools.call(name, args)
                calls.append({"tool": name, "args": args, "ms": round(1000 * (time.time() - t0), 1)})
                results.append(out)
                messages.append({"role": "tool", "tool_call_id": tc.get("id", name), "content": json.dumps(out)})
        else:
            raise LLMError("agent did not finish within the tool-call budget")
        note = (messages[-1]["content"] or "").strip()
        check = verify(note, results)
        if check.unverified and not retried:
            retried = True
            messages.append({"role": "user", "content":
                             "These figures are not in any tool result: " + ", ".join(check.unverified) +
                             ". Rewrite the note using only values copied from tool results (call tools if needed)."})
            continue
        return {"note": note, "tool_calls": calls, "verification": check, "retried": retried}


# ---------------------------------------------------------------- deterministic fallback

def _lots(numbers: list[str]) -> str:
    return ("lot " if len(numbers) == 1 else "lots ") + ", ".join(numbers) if numbers else "no lots"


def template_note(tools: GoldTools, shift_date: str, shift: str) -> dict:
    """Same tools, no LLM: a fixed template. Used when no API key is configured or the LLM fails."""
    calls, results = [], []

    def use(name, **args):
        out = tools.call(name, args)
        calls.append({"tool": name, "args": args, "ms": 0.0})
        results.append(out)
        return out

    k = use("get_shift_kpis", shift_date=shift_date, shift=shift)
    alerts = use("get_shift_alerts", shift_date=shift_date, shift=shift)
    t = k["totals"]
    head = (f"**{len(alerts) or 'No'} drift alert{'s' if len(alerts) != 1 else ''}. "
            f"{t['packs']:,} packs, {t['scrap_rate_pct']}% scrap, CAD {t['giveaway_cost_cad']:.2f} giveaway.**")
    lines = [head, "", "**Issues**"]
    if not alerts:
        lines.append("- Nothing drifted this shift.")
    for a in alerts:
        lines.append(f"- {a['line_id']} {a['signal']} ran {a['direction']} from {a['started']} to {a['ended']}, "
                     f"up to {a['peak_deviation']} {a['unit']} off setpoint (alert at {a['detected']}). "
                     f"Check {_lots(a['lot_numbers'])}."
                     + (f" Probable cause: {a['probable_cause']} ({a['cause_confidence']} confidence)."
                        if a.get("probable_cause") else ""))
    lines += ["", "**Numbers**"]
    for r in k["lines"]:
        lines.append(f"- {r['line_id']}: {r['packs']:,} packs, {r['scrap_rate_pct']}% scrap, "
                     f"{r['giveaway_g_per_pack']} g over label per pack (CAD {r['giveaway_cost_cad']:.2f}).")
    open_alerts = [a for a in alerts if a["status"] == "open"]
    lines += ["", "**For the next shift**",
              f"- Still open: {', '.join(a['line_id'] + ' ' + a['signal'] for a in open_alerts)}." if open_alerts
              else "- No alerts left open."]
    note = "\n".join(lines)
    return {"note": note, "tool_calls": calls, "verification": verify(note, results), "retried": False}


# ---------------------------------------------------------------- entry point

def generate(tools: GoldTools, client: ChatClient | None, shifts: list[tuple[str, str]] | None = None,
             log=print) -> list[dict]:
    shifts = shifts or [(s["shift_date"], s["shift"]) for s in tools.list_shifts()]
    out = []
    for shift_date, shift in shifts:
        writer, model, error = "template", None, None
        t0 = time.time()
        if client is not None:
            try:
                res = run_agent(client, tools, shift_date, shift)
                writer, model = "llm", client.model
            except LLMError as e:
                error = str(e)
                log(f"LLM failed for {shift_date} {shift}, using template: {e}")
                res = template_note(tools, shift_date, shift)
        else:
            res = template_note(tools, shift_date, shift)
        v: Verification = res["verification"]
        rec = {"shift_date": shift_date, "shift": shift, "writer": writer, "model": model,
               "note": res["note"], "figures_total": v.total, "figures_verified": v.verified,
               "unverified": v.unverified, "status": "verified" if not v.unverified else "needs_review",
               "retried": res["retried"], "tool_calls": res["tool_calls"],
               "seconds": round(time.time() - t0, 1), "llm_error": error}
        log(f"{shift_date} {shift}: {writer}{' ' + model if model else ''}, "
            f"{v.verified}/{v.total} figures verified, {len(res['tool_calls'])} tool calls")
        out.append(rec)
    return out


def client_from_env() -> ChatClient | None:
    key = os.environ.get("LLM_API_KEY") or os.environ.get("GEMINI_API_KEY")
    if not key:
        return None
    return ChatClient(base_url=os.environ.get("LLM_BASE_URL", DEFAULT_BASE_URL), api_key=key,
                      model=os.environ.get("LLM_MODEL", DEFAULT_MODEL),
                      min_interval_s=float(os.environ.get("LLM_MIN_INTERVAL_S", "6.5")))
