"""List the Gemini models your API key can call, so you can pick LLM_MODEL.

    python scripts/list_models.py          # reads GEMINI_API_KEY (or LLM_API_KEY) from the environment

The key is sent in a request header, never printed. Free-tier limits differ a lot by model
(Flash-Lite models usually allow far more requests per day than Flash); check them in Google
AI Studio under your project's rate limits.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


def main() -> None:
    key = os.environ.get("GEMINI_API_KEY") or os.environ.get("LLM_API_KEY")
    if not key:
        sys.exit("Set GEMINI_API_KEY in this shell first (export GEMINI_API_KEY=...).")
    url = "https://generativelanguage.googleapis.com/v1beta/models?pageSize=1000"
    req = urllib.request.Request(url, headers={"x-goog-api-key": key})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            models = json.loads(resp.read()).get("models", [])
    except urllib.error.HTTPError as e:
        sys.exit(f"HTTP {e.code}: {e.read()[:300]!r}")
    chat = [m for m in models if "generateContent" in m.get("supportedGenerationMethods", [])]
    for m in sorted(chat, key=lambda m: m["name"]):
        print(f"{m['name'].removeprefix('models/'):45} {m.get('displayName', '')}")
    print(f"\n{len(chat)} models. Use one with: export LLM_MODEL=<name>")


if __name__ == "__main__":
    main()
