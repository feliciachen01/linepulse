"""Score root-cause diagnoses against the simulator's ground truth, over many random days.

Each day (one seed) gets a random set of incidents (random_faults): causes, lines, zones,
directions and sizes all drawn from the seed. Gold tables are built with the pandas
reference (parity-tested against Spark), every alert is investigated, and each diagnosis is
compared with the cause of the fault that raised the alert.

The headline is per incident: the first alert an incident raises is the one a supervisor
investigates. A belt slowdown can raise four alerts (belt plus three hot zones), so scoring
every alert would overweight it; all-alert accuracy is reported too.

    python scripts/evaluate_investigations.py --days 30                    # lookup + rules, ~5 min
    GEMINI_API_KEY=... python scripts/evaluate_investigations.py --days 30 --llm-max 30 \
        --save docs/investigation_eval.json      # then commit it: the dashboard shows this file

Three methods are scored on the same alerts:
    lookup  one cause per alarm signal (oven -> burner, belt -> belt, fill -> filler): what an
            alarm list tells you without investigating
    rules   the deterministic investigator (written by the same person who wrote the simulator,
            so treat it as a sanity check of the evidence, not as a fair opponent)
    llm     the tool-calling agent, told only general plant troubleshooting knowledge

Writes <out>/summary.json and summary.md, <export>/gold_investigation_eval.json for the dashboard and,
with --save, a copy to commit. LLM runs cost free-tier quota, so the published scorecard is the
committed docs/investigation_eval.json rather than a fresh LLM run on every deploy.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from linepulse import reference, simulator  # noqa: E402
from linepulse.handover import client_from_env  # noqa: E402
from linepulse.investigate import (CAUSES, LOOKUP, IncidentTools, investigate,  # noqa: E402
                                   match_truth, rules_investigation)

ORIGIN = datetime.fromisoformat("2026-10-19T06:00:00")
LABELS = [*CAUSES, "unclear"]


def tally(rows: list[dict], key: str) -> dict:
    out = {"n": len(rows), "correct": sum(r[key] == r["truth"] for r in rows)}
    out["rate"] = round(out["correct"] / out["n"], 3) if out["n"] else None
    out["by_cause"] = {c: {"n": sum(r["truth"] == c for r in rows),
                           "correct": sum(r["truth"] == c and r[key] == c for r in rows)} for c in CAUSES}
    out["confusion"] = {t: {p: sum(r["truth"] == t and r[key] == p for r in rows) for p in LABELS} for t in CAUSES}
    return out


def to_markdown(s: dict) -> str:
    m = s["methods"]
    lines = [f"**{s['days']} random simulated days, {s['incidents']} injected incidents** ({s['incidents_detected']} detected, "
             f"{s['false_alarms']} false alarms). Each incident's first alert is investigated and scored against the "
             "injected root cause:", "",
             "| Method | Correct root cause (per incident) |", "| --- | --- |"]
    names = {"lookup": "Alarm lookup table (signal -> cause)", "rules": "Deterministic investigator",
             "llm": f"LLM agent ({s.get('llm_model')})"}
    for k in ("lookup", "rules", "llm"):
        if k in m and m[k]["n"]:
            lines.append(f"| {names[k]} | **{m[k]['correct']} / {m[k]['n']}** ({m[k]['rate']:.0%}) |")
    if "llm" in m and m["llm"]["n"]:
        sub = s["llm_subset"]
        lines += ["", f"On the {m['llm']['n']} alerts the LLM investigated: lookup {sub['lookup']['correct']}/{sub['lookup']['n']}, "
                  f"rules {sub['rules']['correct']}/{sub['rules']['n']}, LLM {m['llm']['correct']}/{m['llm']['n']}. "
                  f"Figures verified in LLM diagnoses: {s['figures']['llm']['verified']}/{s['figures']['llm']['total']}."]
    aa = s["all_alerts"]
    lines += ["", f"Over all {s['alerts_scored']} alerts (a belt slowdown raises up to four): lookup "
              f"{aa['lookup']['correct']}/{aa['lookup']['n']}, rules {aa['rules']['correct']}/{aa['rules']['n']}."]
    lines += ["", "| True cause | Incidents | Lookup | Rules" + (" | LLM" if "llm" in m and m["llm"]["n"] else "") + " |",
              "| --- | --- | --- | ---" + (" | ---" if "llm" in m and m["llm"]["n"] else "") + " |"]
    for c in CAUSES:
        row = f"| {c.replace('_', ' ')} | {m['rules']['by_cause'][c]['n']} | {m['lookup']['by_cause'][c]['correct']} | {m['rules']['by_cause'][c]['correct']}"
        if "llm" in m and m["llm"]["n"]:
            b = m["llm"]["by_cause"][c]
            row += f" | {b['correct']}/{b['n']}"
        lines.append(row + " |")
    return "\n".join(lines) + "\n"


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--days", type=int, default=30, help="Random days to simulate (seeds 1..N)")
    p.add_argument("--llm-max", type=int, default=0, help="Alerts the LLM agent investigates (free tiers are rate limited)")
    p.add_argument("--out", default=str(ROOT / "local_data" / "investigation_eval"))
    p.add_argument("--export", default=str(ROOT / "local_data" / "export"), help="Also write the dashboard copy here")
    p.add_argument("--save", help="Also write the summary here, e.g. docs/investigation_eval.json")
    a = p.parse_args(argv)

    client = client_from_env() if a.llm_max else None
    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    rows, incidents, false_alarms, llm_figs, rules_figs = [], 0, 0, [0, 0], [0, 0]
    t0 = time.time()
    for seed in range(1, a.days + 1):
        faults = simulator.random_faults(seed)
        incidents += len(faults)
        truth = {"origin": ORIGIN.isoformat(), "faults": [asdict(f) | {"cause": f.root_cause, "signals": f.signals,
                                                                       "end_min": f.end_min} for f in faults]}
        gold = reference.gold(list(simulator.generate_events(ORIGIN, 0, 1440, seed=seed, faults=faults)))
        tools = IncidentTools(gold)
        seen = set()
        for alert in sorted(gold["gold_alerts"], key=lambda x: x["detected_at"]):
            t = match_truth(alert, truth)
            if t is None:
                false_alarms += 1
                continue
            first = t["fault_id"] not in seen
            seen.add(t["fault_id"])
            rules = rules_investigation(tools, alert["alert_id"])
            rules_figs[0] += rules["verification"].verified
            rules_figs[1] += rules["verification"].total
            row = {"seed": seed, "alert_id": alert["alert_id"], "fault_id": t["fault_id"], "truth": t["cause"], "first": first,
                   "lookup": LOOKUP.get(alert["signal"], "unclear"), "rules": rules["diagnosis"]["root_cause"]}
            if first and client is not None and sum("llm" in r for r in rows) < a.llm_max:
                rec = investigate(tools, alert["alert_id"], client, truth, log=lambda *x: None)
                if rec["writer"] == "llm":
                    row["llm"] = rec["diagnosis"]["root_cause"]
                    row["llm_status"] = rec["status"]
                    llm_figs[0] += rec["figures_verified"]
                    llm_figs[1] += rec["figures_total"]
            rows.append(row)
        print(f"day {seed}: {len(faults)} incidents, {len(gold['gold_alerts'])} alerts "
              f"(rules {sum(r['rules'] == r['truth'] for r in rows)}/{len(rows)} so far, {time.time() - t0:.0f} s)")

    llm_rows = [r for r in rows if "llm" in r]
    firsts = [r for r in rows if r["first"]]
    summary = {
        "days": a.days, "incidents": incidents, "incidents_detected": len(firsts), "alerts_scored": len(rows),
        "false_alarms": false_alarms,
        "methods": {"lookup": tally(firsts, "lookup"), "rules": tally(firsts, "rules")},
        "all_alerts": {"lookup": tally(rows, "lookup"), "rules": tally(rows, "rules")},
        "figures": {"rules": {"verified": rules_figs[0], "total": rules_figs[1]}},
        "llm_model": client.model if client else None,
    }
    if llm_rows:
        summary["methods"]["llm"] = tally(llm_rows, "llm")
        summary["llm_subset"] = {"lookup": tally(llm_rows, "lookup"), "rules": tally(llm_rows, "rules")}
        summary["figures"]["llm"] = {"verified": llm_figs[0], "total": llm_figs[1]}
    md = to_markdown(summary)
    (out / "summary.json").write_text(json.dumps({"summary": summary, "rows": rows}, indent=2))
    (out / "summary.md").write_text(md)
    if a.export and Path(a.export).exists():
        (Path(a.export) / "gold_investigation_eval.json").write_text(json.dumps(summary, indent=2))
    if a.save:
        Path(a.save).write_text(json.dumps(summary, indent=2) + "\n")
    print("\n" + md)


if __name__ == "__main__":
    main()
