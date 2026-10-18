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
injected in the first shift only, so the other 16 hours are a clean false-alarm test.

| Metric | Live result |
| --- | --- |
| Faults detected | **3 / 3** |
| Detection latency | mean **8.3 min** |
| False alarms over 24 h (defaults 5 °C / 1.0 g) | **0** |
| Last incremental pipeline run (bronze → silver → gold) | 56.3 s on serverless |
| Matches the locally tested pipeline | **identical**: all 15 threshold-sweep rows equal a local run of the same 1,440 minutes |

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
pytest -q                                                       # 14 tests, ~2 min
cd api/LinePulse.Api && dotnet run                              # open http://localhost:5000 (port printed on start)
```

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

- 14 pytest tests: chunked simulation equals a single run, DQ rules name every failure,
  end-to-end detection + idempotent rerun, Spark and pandas detectors agree, shipped
  functions pickle without importable modules, sweep handles runs with no alerts.
- The pipeline also runs over **Spark Connect** with workers that cannot import the
  package, the same condition as Databricks serverless.
- API: 12 smoke checks in Local mode, and in Databricks mode against a fake Statement
  Execution API (`tests/fake_databricks.py`) that returns the real response shape.
- Live on Databricks Free Edition serverless: results identical to the local run (above).
  The API has not yet been pointed at the live SQL warehouse.

## Next steps

1. Azure Event Hubs (Kafka endpoint) as the ingestion source instead of files.
2. A second line whose feed uses different field names, to exercise `_rescued_data`
   schema-drift quarantine (the legacy-plant integration problem).
3. MLflow-tracked anomaly model compared against the EWMA baseline on the same scorecard.
4. Shift-handover summary written by an LLM that may only cite gold-table queries.

## Layout

```
src/linepulse/   simulator, pipeline (bronze/silver/gold), store (UC or local), evaluate
notebooks/       Databricks notebooks: 00 setup, 01 simulate, 02 pipeline, 03 evaluate, 99 reset
scripts/         run_local.py (whole pipeline on your machine), evaluate_seeds.py, smoke_api.sh
tests/           pytest suite + fake Databricks SQL endpoint
api/             .NET 8 minimal API + dashboard (wwwroot)
databricks.yml   optional Asset Bundle for the scheduled job
```
