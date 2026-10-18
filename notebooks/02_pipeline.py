# Databricks notebook source
# MAGIC %md
# MAGIC # LinePulse 02 - bronze -> silver -> gold
# MAGIC * **Bronze**: Auto Loader picks up only new files (Trigger.AvailableNow - the trigger serverless supports).
# MAGIC * **Silver**: data-quality rules; failures go to `silver_quarantine`; MERGE on `event_id` removes re-sends.
# MAGIC * **Gold**: per-minute signals with EWMA, drift alerts with affected lots, shift KPIs, line status, DQ summary.

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../src"))

from linepulse import pipeline
from linepulse.store import TableStore

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "linepulse")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")

store = TableStore.unity_catalog(spark, catalog, schema, volume="raw")
landing = f"/Volumes/{catalog}/{schema}/raw/landing"

# COMMAND ----------

import time
t0 = time.time()
counts = pipeline.run_all(store, landing, use_autoloader=True)
print(f"pipeline finished in {time.time() - t0:.1f} s")
display(spark.createDataFrame([(k, v) for k, v in counts.items()], ["table", "rows"]))

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.gold_alerts"))

# COMMAND ----------

display(spark.table(f"{catalog}.{schema}.gold_shift_kpis"))
