"""Investigate every alert in the gold export and record each investigation step by step.

    python scripts/run_local.py --minutes 1440             # gold tables -> local_data/export
    GEMINI_API_KEY=... python scripts/investigate_alerts.py # or no key: deterministic investigator

With an API key the LLM agent investigates through tool calls; without one (or if the API
fails) the deterministic investigator does, with the same tools. Each record keeps the steps
(why, which tool, what came back), the diagnosis, the figure check and, because the data is
simulated, the injected root cause to compare with.

Writes <export>/gold_investigations.json, which the .NET API and the static demo replay, and
copies the committed diagnosis-accuracy scorecard (docs/investigation_eval.json) next to it.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from linepulse.handover import client_from_env  # noqa: E402
from linepulse.investigate import IncidentTools, investigate  # noqa: E402


def primary_alerts(alerts: list[dict], gap_min: int = 10) -> set[str]:
    """The first alert of each incident. Alerts on one line that overlap (or follow within
    gap_min) are one incident: a slowing belt raises a belt alarm plus several hot zones.
    Same grouping as the dashboard, which only shows each incident's first alert."""
    ts = lambda s: datetime.fromisoformat(s.replace("Z", "+00:00"))
    firsts, open_until = set(), {}
    for a in sorted(alerts, key=lambda a: a["start_minute"]):
        start, end = ts(a["start_minute"]), ts(a["end_minute"])
        until = open_until.get(a["line_id"])
        if until is None or start > until + timedelta(minutes=gap_min):
            firsts.add(a["alert_id"])
            open_until[a["line_id"]] = end
        else:
            open_until[a["line_id"]] = max(until, end)
    return firsts


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--export", default=str(ROOT / "local_data" / "export"))
    p.add_argument("--truth", default=str(ROOT / "local_data" / "ground_truth" / "faults.json"))
    p.add_argument("--no-llm", action="store_true", help="Use the deterministic investigator even if a key is set")
    p.add_argument("--llm-budget-s", type=float, default=480,
                   help="Stop starting LLM investigations after this many seconds (the rest use rules), "
                        "so a slow or rate-limited API can never hang a deploy")
    a = p.parse_args(argv)
    export = Path(a.export)
    tools = IncidentTools.from_export(export)
    truth = json.loads(Path(a.truth).read_text()) if Path(a.truth).exists() else None
    client = None if a.no_llm else client_from_env()
    print(f"investigator: {'LLM ' + client.model if client else 'rules (no API key)'}")
    # The LLM investigates each incident's first alert (the one the dashboard shows); the other
    # alarms in an incident, and anything past the time budget, use the deterministic investigator.
    firsts = primary_alerts(tools.alerts.to_dict(orient="records"))
    t0, recs = time.time(), []
    for alert_id in tools.alerts.sort_values("start_minute").alert_id:
        use_llm = client is not None and alert_id in firsts and time.time() - t0 < a.llm_budget_s
        recs.append(investigate(tools, alert_id, client if use_llm else None, truth))
    if client is not None:
        print(f"LLM investigated {sum(r['writer'] == 'llm' for r in recs)} of {len(firsts)} incidents "
              f"in {time.time() - t0:.0f} s")
    (export / "gold_investigations.json").write_text(json.dumps(recs, indent=2, ensure_ascii=False, default=str))
    scorecard = ROOT / "docs" / "investigation_eval.json"
    if scorecard.exists():
        shutil.copy(scorecard, export / "gold_investigation_eval.json")
    scored = [r for r in recs if r["ground_truth"]]
    right = sum(r["diagnosis"]["root_cause"] == r["ground_truth"]["cause"] for r in scored)
    print(f"{len(recs)} investigations, {sum(r['status'] == 'verified' for r in recs)} fully verified, "
          f"{right}/{len(scored)} match the injected cause -> {export / 'gold_investigations.json'}")


if __name__ == "__main__":
    main()
