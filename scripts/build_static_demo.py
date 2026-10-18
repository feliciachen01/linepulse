"""Build a static, read-only copy of the dashboard for GitHub Pages.

Takes the gold tables exported by run_local.py and writes them as the exact JSON the
.NET API would return, one file per endpoint, next to the same index.html the API
serves. No server, no Databricks token: safe to publish.

    python scripts/run_local.py --minutes 1440
    python scripts/build_static_demo.py            # -> site/

The default run (seed 7, 1,440 minutes, default thresholds) is the run that was executed
live on Databricks Free Edition; its gold tables matched row for row, so the page says so.
"""

from __future__ import annotations

import argparse
import html
import json
import shutil
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PAGE = ROOT / "api" / "LinePulse.Api" / "wwwroot" / "index.html"
LIVE_RUN_MINUTES = 1440  # length of the live Databricks run the snapshot can be compared with


def load(export: Path, table: str) -> list[dict]:
    return json.loads((export / f"{table}.json").read_text())


def build(export: Path, out: Path, repo_url: str) -> dict:
    scorecard = json.loads((export / "scorecard.json").read_text())
    lines = sorted(load(export, "gold_line_status"), key=lambda r: r["line_id"])
    alerts = sorted(load(export, "gold_alerts"), key=lambda r: r["start_minute"], reverse=True)[:50]
    shifts = sorted(load(export, "gold_shift_kpis"), key=lambda r: (r["line_id"], r["shift"]))
    shifts = sorted(shifts, key=lambda r: r["shift_date"], reverse=True)  # API order: date desc, line, shift
    dq = sorted(load(export, "gold_dq_summary"), key=lambda r: -r["records"])
    signals = defaultdict(list)
    for r in load(export, "gold_signal_minute"):
        signals[(r["line_id"], r["signal"])].append(r)

    minutes = max(len(rows) for rows in signals.values())  # one row per signal per simulated minute
    meta = {
        "hours": round(minutes / 60),
        "matches_live": minutes == LIVE_RUN_MINUTES,
        "repo": repo_url,
        "scorecard": {k: scorecard[k] for k in ("faults", "detected", "mean_latency_min", "false_alarms")},
    }

    if out.exists():
        shutil.rmtree(out)
    data = out / "data"
    (data / "signals").mkdir(parents=True)

    def write(rel: str, obj) -> None:
        path = data / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, separators=(",", ":")))

    write("health.json", {"status": "ok", "mode": "snapshot"})
    write("lines.json", lines)
    write("alerts.json", alerts)
    write("shifts.json", shifts)
    write("data-quality.json", dq)
    for (line, signal), rows in signals.items():
        write(f"signals/{line}/{signal}.json", sorted(rows, key=lambda r: r["minute"]))

    page = PAGE.read_text()
    inject = f"<script>window.LINEPULSE_STATIC = {json.dumps(meta)};</script>\n<script>"
    assert page.count("<script>") == 1, "index.html layout changed: update the injection point"
    page = page.replace("<script>", inject, 1)
    page = page.replace("<title>LinePulse</title>",
                        "<title>LinePulse</title>\n<meta name=\"description\" content=\""
                        + html.escape("Static snapshot of the LinePulse bakery-line dashboard") + "\">", 1)
    (out / "index.html").write_text(page)
    (out / ".nojekyll").write_text("")  # serve files as-is on GitHub Pages
    return {"files": sum(1 for _ in out.rglob("*") if _.is_file()), **meta}


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--export", default=str(ROOT / "local_data" / "export"))
    p.add_argument("--out", default=str(ROOT / "site"))
    p.add_argument("--repo", default="https://github.com/feliciachen01/linepulse")
    a = p.parse_args(argv)
    print(json.dumps(build(Path(a.export), Path(a.out), a.repo), indent=2))


if __name__ == "__main__":
    main()
