"""Write shift-handover notes for every shift in the gold export.

    python scripts/run_local.py --minutes 1440          # gold tables -> local_data/export
    GEMINI_API_KEY=... python scripts/generate_handover.py

With an API key the LLM agent writes each note through tool calls; without one (or if the
API fails) a deterministic template writes it. Either way every figure is verified against
the tool results and the outcome is recorded next to the note.

Any OpenAI-compatible endpoint works: set LLM_BASE_URL, LLM_MODEL and LLM_API_KEY.
Writes <export>/gold_handover.json, which the .NET API and the static demo read.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from linepulse.handover import GoldTools, client_from_env, generate  # noqa: E402


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--export", default=str(ROOT / "local_data" / "export"))
    p.add_argument("--no-llm", action="store_true", help="Use the template writer even if a key is set")
    a = p.parse_args(argv)
    export = Path(a.export)
    tools = GoldTools.from_export(export)
    client = None if a.no_llm else client_from_env()
    print(f"writer: {'LLM ' + client.model if client else 'template (no API key)'}")
    notes = generate(tools, client)
    (export / "gold_handover.json").write_text(json.dumps(notes, indent=2))
    ok = sum(n["status"] == "verified" for n in notes)
    figs = sum(n["figures_total"] for n in notes)
    good = sum(n["figures_verified"] for n in notes)
    print(f"{len(notes)} notes, {ok} fully verified, {good}/{figs} figures verified -> {export / 'gold_handover.json'}")


if __name__ == "__main__":
    main()
