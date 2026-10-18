# LinePulse

Real-time quality and waste monitoring for a (simulated) bakery line: a Databricks
lakehouse that turns line telemetry into drift alerts, shift KPIs and recall-ready
lot lists, served by a .NET 8 API and dashboard. Runs entirely on free tiers.

> **Simulated data.** Two naan lines, three injected faults, deliberate bad records
> and duplicate deliveries. Because every fault is injected on purpose, detection is
> *scored*, not claimed. All plant numbers are illustrative assumptions, not FGF data.

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

**Threshold sweep** (`python scripts/run_local.py --sweep`): lowering the oven threshold
from 5 °C to 4 °C keeps 0 false alarms and cuts mean latency to 8.0 min; lowering the fill
threshold to 0.6 g buys ~1 min but costs 5 false alarms per shift; 3 °C costs 23-28.
The defaults (5 °C, 1.0 g) are chosen to keep false alarms at zero, because supervisors
stop trusting a noisy alarm.

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
pytest -q                                                       # 7 tests, ~2 min
cd api/LinePulse.Api && dotnet run                              # open http://localhost:5000 (port printed on start)
```

If Spark cannot download the Delta jar (no access to Maven), add `--engine parquet`;
everything runs the same, with MERGE emulated by an anti-join.

## Run it on Databricks Free Edition

1. Sign up at Databricks Free Edition (no credit card). Non-commercial use only.
2. **Workspace > Create > Git folder**, paste this repo's URL.
3. Run `notebooks/00_setup` once (creates `workspace.linepulse` and the `raw` volume).
4. Run `01_simulate` (default 60 minutes of data), then `02_pipeline`, then `03_evaluate`.
   Repeat 01 → 02 to watch alerts appear incrementally.
5. Optional: **Jobs > Create job** with tasks 01 → 02 → 03 on a 15-minute schedule, or
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
- **Why cache in the API?** Gold only changes when the pipeline runs; each cache hit is a
  query the Free Edition warehouse does not run.

## Tested here

- 7 pytest tests: chunked simulation equals a single run, DQ rules name every failure,
  end-to-end detection + idempotent rerun, Spark and pandas detectors agree.
- The pipeline also runs over **Spark Connect** with workers that cannot import the
  package, the same condition as Databricks serverless.
- API: 12 smoke checks in Local mode, and in Databricks mode against a fake Statement
  Execution API (`tests/fake_databricks.py`) that returns the real response shape.
- Not yet run on a live Databricks workspace: do that first (steps above).

## Next steps

1. Azure Event Hubs (Kafka endpoint) as the ingestion source instead of files.
2. A second line whose feed uses different field names, to exercise `_rescued_data`
   schema-drift quarantine (the legacy-plant integration problem).
3. MLflow-tracked anomaly model compared against the EWMA baseline on the same scorecard.
4. Multi-seed evaluation (latency and false alarms as distributions, not one shift).
5. Shift-handover summary written by an LLM that may only cite gold-table queries.

## Layout

```
src/linepulse/   simulator, pipeline (bronze/silver/gold), store (UC or local), evaluate
notebooks/       Databricks notebooks: 00 setup, 01 simulate, 02 pipeline, 03 evaluate, 99 reset
scripts/         run_local.py (whole pipeline on your machine), smoke_api.sh
tests/           pytest suite + fake Databricks SQL endpoint
api/             .NET 8 minimal API + dashboard (wwwroot)
databricks.yml   optional Asset Bundle for the scheduled job
```
