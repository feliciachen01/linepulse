# LinePulse

[![ci](https://github.com/feliciachen01/linepulse/actions/workflows/ci.yml/badge.svg)](https://github.com/feliciachen01/linepulse/actions/workflows/ci.yml)
[![pages](https://github.com/feliciachen01/linepulse/actions/workflows/pages.yml/badge.svg)](https://feliciachen01.github.io/linepulse/)

Real-time quality and waste monitoring for a (simulated) bakery line: a Databricks
lakehouse that turns line telemetry into drift alerts, shift KPIs and recall-ready
lot lists, served by a .NET 8 API and dashboard. Runs entirely on free tiers.

**[Open the live demo](https://feliciachen01.github.io/linepulse/)**: a static snapshot of the
dashboard over 24 simulated hours. Click an alert to chart the excursion behind it.

[![LinePulse dashboard: line status, the shift-handover note with every figure checked, drift alerts](docs/dashboard.png)](https://feliciachen01.github.io/linepulse/)

- **Live on Databricks Free Edition:** 3/3 injected faults caught, mean 8.3 min, 0 false alarms in 24 h
- **Measured, not claimed:** 30/30 faults across 10 simulated shifts; every bad record quarantined
- **Shift-handover agent:** an LLM writes the end-of-shift note through tool calls over the gold
  tables, and a checker confirms every number, time and lot ID it writes came from those tools
- **Stack:** Auto Loader, Delta, Unity Catalog, Spark Structured Streaming, .NET 8 minimal API, GitHub Actions

> **Simulated data.** Two naan lines, injected faults with known root causes, deliberate
> bad records and duplicate deliveries. Because every fault is injected on purpose, detection
> is *scored*, not claimed. All plant numbers are illustrative assumptions, not FGF data.

### The simulated day

Each fault has a root cause, and each cause leaves a different footprint across the line's
signals. That matters because the same alarm, "oven zone 2 running hot", can have three causes:

| Root cause | What the sensors show | What the product shows |
| --- | --- | --- |
| Burner fault | one zone hot or cold | bake defects rise |
| Thermocouple drift | one zone reads hot or cold, and noisier | nothing changes: the oven is fine |
| Belt slowdown | belt speed drops, every zone reads hotter (less dough load) | fewer packs per minute, bake defects rise |
| Filler drift | pack weight moves | giveaway (heavy) or underweight rejects (light) |

The default 24-hour day (seed 7) injects eight faults. Shift A's three are unchanged from the
first release, so its results below stay comparable; shifts B and C add five more, including
two lines alarming on a hot oven zone 13 minutes apart for different reasons (a drifting
thermocouple on L1, a slowing belt on L2). Over the full day: **8 / 8 faults detected, mean
10.4 min, 0 false alarms**. The slowest catch is the drifting thermocouple (23 min): it creeps
up slowly by design.

## Results (one simulated 8-hour shift, 86,879 events)

| What | Result | How it is measured |
| --- | --- | --- |
| Faults detected | **3 / 3** | `scorecard.json` vs `ground_truth/faults.json` |
| Detection latency | **mean 8.3 min, max 12 min** after fault onset | first alert's `detected_at` - fault onset |
| False alarms | **0** | alerts that overlap no injected fault |
| Bad records quarantined | **179 / 179** | `silver_quarantine` vs injected count |
| Duplicate deliveries removed | **479 / 479** | bronze - silver - quarantine |
| Incremental = batch | **identical** gold tables from 1 run or 8 runs | `run_local.py --chunks 8` |
| Local pipeline run | ~31 s per shift (laptop, Spark local mode) | wall clock |

Per fault: slow oven-zone drift (+15 °C over 20 min) caught in 12 min; filler creeping
3 g heavy caught in 10 min; burner step (-12 °C) caught in 3 min. The 10-minute target
was missed for the slow ramp. That is a property of the EWMA + 3-minute confirmation
rule, and the threshold sweep below shows the trade-off.

### Across 10 shifts (`python scripts/evaluate_seeds.py --seeds 10`)

One shift is an anecdote, so the full pipeline is rerun for 10 simulator seeds: the same
three faults, with different sensor noise and process wander each time.

| Metric | Result |
| --- | --- |
| Faults detected | **30 / 30** |
| Detection latency | mean **8.5 min**, p50 9, p90 13, max 14 |
| False alarms | **0** in all 10 shifts |
| Quarantine and dedup exact | in every shift |

| Fault | Fastest | Slowest |
| --- | --- | --- |
| F1 slow oven drift (+15 °C over 20 min) | 7 min | 14 min |
| F2 filler creeping 3 g heavy | 8 min | 11 min |
| F3 burner step (-12 °C) | 3 min | 5 min |

**Threshold trade-off** (false alarms summed over the 10 shifts):

| Oven / fill threshold | Mean latency | False alarms |
| --- | --- | --- |
| 3 °C / 1.0 g | 7.0 min | 182 |
| 4 °C / 1.0 g | 7.9 min | 4 |
| **5 °C / 1.0 g (default)** | **8.5 min** | **0** |
| 5 °C / 0.6 g | 7.7 min | 51 |
| 8 °C / 1.0 g | 10.4 min | 0 |

The single-shift sweep made 4 °C look free (0 false alarms). Across 10 shifts it costs 4,
which is why the default is 5 °C: supervisors stop trusting an alarm that cries wolf, and
the extra 0.6 min of latency is cheap by comparison.

### Live run on Databricks Free Edition

The same pipeline, run on Databricks serverless (Auto Loader, Delta, Unity Catalog) in
several incremental chunks, covering **24 simulated hours**: three shifts, with the faults
injected in the first shift only, so the other 16 hours are a clean false-alarm test. (This
was the default day before root causes were added; shift A is identical in both.)

| Metric | Live result |
| --- | --- |
| Events ingested | 260,543 |
| Faults detected | **3 / 3** |
| Detection latency | mean **8.3 min**, max 12 (F1 12, F2 10, F3 3) |
| False alarms over 24 h (defaults 5 °C / 1.0 g) | **0** |
| Bad records quarantined | **526 / 526** |
| Duplicate deliveries removed | **1,343 / 1,343** |
| Last incremental pipeline run (bronze → silver → gold) | 56.3 s on serverless |
| Matches the locally tested pipeline | **identical**: scorecard and all 15 threshold-sweep rows equal a local run of the same 1,440 minutes |

| Oven / fill threshold | Mean latency | False alarms in 24 h |
| --- | --- | --- |
| 3 °C / 1.0 g | 7.3 min | 51 |
| 4 °C / 1.0 g | 8.0 min | 2 |
| **5 °C / 1.0 g (default)** | **8.3 min** | **0** |
| 5 °C / 0.6 g | 7.3 min | 19 |
| 8 °C / 1.0 g | 9.3 min | 0 |

Getting there took two serverless-only fixes the local tests could not see, both now
covered by regression tests: functions shipped to workers must not reference a module
imported from a Git folder (its workspace loader cannot be pickled), and the sweep must
handle a run where no fault has started yet.

### Live dashboard (.NET 8 API on the Databricks SQL warehouse)

[This screenshot](docs/dashboard-live.png) is the API on the live warehouse (earlier page design). The API runs in a GitHub Codespace and reads the gold tables through the Databricks SQL
Statement Execution API with a personal access token. The dashboard shows both lines, the
three drift alerts with the lots made during each excursion, and KPIs for all three shifts.

| Request (`GET /api/alerts`) | Time |
| --- | --- |
| First call: live query to the SQL warehouse | 1.40 s |
| Next calls: served from the API's 60 s cache | 2-5 ms |

The cache is there because gold only changes when the pipeline runs; every cache hit is a
query the Free Edition warehouse does not have to run.

## Shift-handover agent

At the end of each shift the incoming supervisor needs four things: what drifted, when, which
lots to check, and how the shift went. The agent writes that note.

1. **Tools, not raw data.** The model gets five read-only tools over the gold tables
   (`list_shifts`, `get_shift_kpis`, `get_shift_alerts`, `get_line_status`, `get_data_quality`)
   and is told to copy figures from them, never compute or estimate.
2. **Every figure is checked.** After the model answers, a checker extracts every number, clock
   time, date and lot ID from the note and matches it against the tool results (allowing only the
   rounding the note shows, so 17.27 may be written 17.3).
3. **One correction round.** If a figure is not found, the model is told which ones and asked to
   rewrite once. Still wrong: the note is published marked "needs review", with the bad figures
   highlighted in red on the dashboard.
4. **Graceful fallback.** With no API key, or if the API fails, a fixed template writes the note
   from the same tools, and the dashboard says so.

The dashboard shows the result: checked figures have a dotted underline, the header says how many
were checked, and "How this note was written and checked" lists the tool calls.

```bash
python scripts/run_local.py --minutes 1440            # gold tables
GEMINI_API_KEY=... python scripts/generate_handover.py   # or no key: template writer
```

**Model.** It uses any OpenAI-compatible chat API (`LLM_BASE_URL`, `LLM_MODEL`, `LLM_API_KEY`).
The default is Google's Gemini free tier (`gemini-2.5-flash`), which needs no credit card.
To use it for the live demo, create a key at Google AI Studio and add it to the repo under
**Settings > Secrets and variables > Actions** as `GEMINI_API_KEY`. The `pages` workflow then
writes the notes with the LLM on every deploy. Free-tier prompts may be used by Google to
improve its products; the data here is simulated. Databricks Free Edition cannot call LLM APIs,
so notes are generated outside Databricks and the API serves them from a file in both modes.

## How it works

```
simulator ──JSON files──> landing (UC volume)
                              │  Auto Loader, Trigger.AvailableNow
                              ▼
                        bronze_events          raw + file + ingest time, append-only
                              │  foreachBatch: DQ rules, MERGE on event_id
                 ┌────────────┼──────────────┐
                 ▼            ▼              ▼
        silver_telemetry  silver_units  silver_quarantine (failed rule names)
                 └──────┬─────┘
                        ▼  per-minute means -> EWMA (applyInPandas) -> gaps-and-islands
   gold_signal_minute · gold_alerts (+ affected lots) · gold_shift_kpis · gold_line_status · gold_dq_summary
                        │  Databricks SQL Statement Execution API (parameterized, cached 60 s)
                        ▼
              .NET 8 minimal API  ──>  dashboard (served by the API)
```

## Run it locally (no cloud account needed)

Needs Python 3.11+, Java 17+, .NET 8 SDK - or open the repo in **GitHub Codespaces**
(the devcontainer installs all three).

```bash
pip install -r requirements.txt
python scripts/run_local.py --minutes 480 --chunks 8 --sweep   # simulate a shift, build gold, print scorecard
pytest -q                                                       # 23 tests, ~2 min
cd api/LinePulse.Api && dotnet run                              # open http://localhost:5000 (port printed on start)
```

To rebuild the static demo: `python scripts/run_local.py --minutes 1440` then
`python scripts/build_static_demo.py` (writes `site/`). The `pages` workflow does this on every
push to `main` and publishes it to GitHub Pages.

If Spark cannot download the Delta jar (no access to Maven), add `--engine parquet`;
everything runs the same, with MERGE emulated by an anti-join.

## Run it on Databricks Free Edition

1. Sign up at Databricks Free Edition (no credit card). Non-commercial use only.
2. **Workspace > Create > Git folder**, paste this repo's URL.
3. Run `notebooks/00_setup` once (creates `workspace.linepulse` and the `raw` volume).
4. Attach each notebook to **Serverless** compute (the only option on Free Edition).
5. Run `01_simulate` (default 60 minutes of data), then `02_pipeline`, then `03_evaluate`.
   Repeat 01 → 02 to watch alerts appear incrementally. Set `minutes` = 480 for a full
   shift. Changing a widget can re-run the notebook: to stop that, set the widget panel's
   gear icon to "Do nothing" on widget change. Each run of 01 appends more data.
6. Optional: **Jobs > Create job** with tasks 01 → 02 → 03 on a 15-minute schedule, or
   deploy `databricks.yml` with the Databricks CLI. Keep it paused when you are not demoing:
   going over Free Edition quota stops compute until the quota resets.

### Point the API at Databricks

Create a personal access token (**Settings > Developer > Access tokens**) and copy your SQL
warehouse ID (**SQL Warehouses > your warehouse > Connection details**). Then:

```bash
export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<token>          # never commit this
export DATABRICKS_WAREHOUSE_ID=<id>
LinePulse__Mode=Databricks dotnet run --project api/LinePulse.Api
```

## API

| Endpoint | Returns |
| --- | --- |
| `GET /health` | `{status, mode}` |
| `GET /api/lines` | status per line: latest zone temps, fill weight, 15-min scrap, open alerts |
| `GET /api/lines/{lineId}/status` | one line (404 if unknown, 400 if malformed) |
| `GET /api/alerts?lineId=&status=open\|closed&limit=` | drift alerts with affected lots and packs in window |
| `GET /api/shifts?lineId=` | shift KPIs: scrap %, rejects by reason, giveaway g/pack, kg, CAD |
| `GET /api/signals/{lineId}/{signal}?minutes=` | per-minute mean, EWMA deviation, threshold |
| `GET /api/data-quality` | quarantined records by rule |
| `GET /api/handover` | shift-handover notes with figures checked/total, unverified figures, tool calls |

Inputs are validated against allow-lists and all SQL is parameterized.

## Design decisions (and the questions they answer)

- **Why medallion layers?** Bronze keeps exactly what arrived, so silver logic can change
  and be replayed without re-ingesting. Gold is the only layer the API may read.
- **Why micro-batch, not continuous streaming?** Serverless supports only
  `Trigger.AvailableNow`. Minutes of latency suits scrap and giveaway decisions; a safety
  interlock would belong in the PLC, not the lakehouse.
- **Why quarantine instead of drop?** A QA lead can audit every rejected record and the
  rule it failed. "Silently dropped" is how plants lose trust in data.
- **Why MERGE on `event_id`?** Gateways deliver at-least-once. MERGE makes reruns and
  re-sent events idempotent, which the tests prove.
- **Why EWMA before ML?** No labels needed, explainable to plant staff, and it sets the
  baseline any model must beat. The sweep makes the threshold a business decision.
- **Why affected lots on every alert?** Recall scope is the expensive question after an
  excursion; answering it in seconds is the point.
- **Why check the LLM's numbers instead of trusting it?** A handover with one wrong lot number
  is worse than no handover. Grounding the model in tools narrows what it can say; the checker
  proves what it did say, and the result is visible to the supervisor, not hidden in a log.
- **Why cache in the API?** Gold only changes when the pipeline runs; each cache hit is a
  query the Free Edition warehouse does not run.

## Tested here

- 23 pytest tests: chunked simulation equals a single run, DQ rules name every failure,
  end-to-end detection + idempotent rerun, Spark and pandas detectors agree, shipped
  functions pickle without importable modules, sweep handles runs with no alerts,
  static demo files match the API's responses, and the handover agent (against a fake LLM
  server): tool use, a hallucinated figure caught and fixed on retry, one left wrong marked for
  review, API failure falling back to the template.
- The pipeline also runs over **Spark Connect** with workers that cannot import the
  package, the same condition as Databricks serverless.
- API: 12 smoke checks in Local mode, and in Databricks mode against a fake Statement
  Execution API (`tests/fake_databricks.py`) that returns the real response shape.
- Live on Databricks Free Edition serverless: results identical to the local run (above).
  The .NET API has served the live gold tables from the Databricks SQL warehouse (above).

## Next steps

1. Azure Event Hubs (Kafka endpoint) as the ingestion source instead of files.
2. A second line whose feed uses different field names, to exercise `_rescued_data`
   schema-drift quarantine (the legacy-plant integration problem).
3. MLflow-tracked anomaly model compared against the EWMA baseline on the same scorecard.
4. Score the handover agent over many runs (share of notes fully verified, retries needed),
   the same way the detector is scored.

## Layout

```
src/linepulse/   simulator, pipeline (bronze/silver/gold), store (UC or local), evaluate, handover (agent)
notebooks/       Databricks notebooks: 00 setup, 01 simulate, 02 pipeline, 03 evaluate, 99 reset
scripts/         run_local.py (whole pipeline on your machine), evaluate_seeds.py, generate_handover.py,
                 build_static_demo.py, smoke_api.sh
tests/           pytest suite + fake Databricks SQL endpoint
api/             .NET 8 minimal API + dashboard (wwwroot)
databricks.yml   optional Asset Bundle for the scheduled job
```
