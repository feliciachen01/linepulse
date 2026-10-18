"""Investigate every alert in the gold export and record each investigation step by step.

    python scripts/run_local.py --minutes 1440             # gold tables -> local_data/export
    GEMINI_API_KEY=... python scripts/investigate_alerts.py # or no key: deterministic investigator

With an API key the LLM agent investigates through tool calls; without one (or if the API
fails) the deterministic investigator does, with the same tools. Each record keeps the steps
(why, which tool, what came back), the diagnosis, the figure check and, because the data is
simulated, the injected root cause to compare with.

Writes <export>/gold_investigations.json, which the .NET API and the static demo replay.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from linepulse.handover import client_from_env  # noqa: E402
from linepulse.investigate import IncidentTools, investigate  # noqa: E402


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--export", default=str(ROOT / "local_data" / "export"))
    p.add_argument("--truth", default=str(ROOT / "local_data" / "ground_truth" / "faults.json"))
    p.add_argument("--no-llm", action="store_true", help="Use the deterministic investigator even if a key is set")
    a = p.parse_args(argv)
    export = Path(a.export)
    tools = IncidentTools.from_export(export)
    truth = json.loads(Path(a.truth).read_text()) if Path(a.truth).exists() else None
    client = None if a.no_llm else client_from_env()
    print(f"investigator: {'LLM ' + client.model if client else 'rules (no API key)'}")
    recs = [investigate(tools, alert_id, client, truth)
            for alert_id in tools.alerts.sort_values("start_minute").alert_id]
    (export / "gold_investigations.json").write_text(json.dumps(recs, indent=2, ensure_ascii=False, default=str))
    scored = [r for r in recs if r["ground_truth"]]
    right = sum(r["diagnosis"]["root_cause"] == r["ground_truth"]["cause"] for r in scored)
    print(f"{len(recs)} investigations, {sum(r['status'] == 'verified' for r in recs)} fully verified, "
          f"{right}/{len(scored)} match the injected cause -> {export / 'gold_investigations.json'}")


if __name__ == "__main__":
    main()
