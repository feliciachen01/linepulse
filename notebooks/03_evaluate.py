# Databricks notebook source
# MAGIC %md
# MAGIC # LinePulse 03 - score detection against ground truth
# MAGIC Detection latency per injected fault, false alarms, data-quality accounting,
# MAGIC and the threshold sweep (latency vs false alarms) to discuss in interviews.

# COMMAND ----------

import os, sys, json
sys.path.append(os.path.abspath("../src"))

from linepulse import evaluate

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "linepulse")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
root = f"/Volumes/{catalog}/{schema}/raw"
T = lambda t: f"{catalog}.{schema}.{t}"

truth = json.loads(open(f"{root}/ground_truth/faults.json").read())
cursor = json.loads(open(f"{root}/ground_truth/cursor.json").read())
dq_truth = json.loads(open(f"{root}/ground_truth/dq_truth.json").read())

# COMMAND ----------

from datetime import datetime, timedelta
data_until = datetime.fromisoformat(cursor["origin"]) + timedelta(minutes=cursor["next_min"])
score = evaluate.score_alerts(spark.table(T("gold_alerts")).toPandas(), truth, data_until=data_until)

origin = datetime.fromisoformat(cursor["origin"])
first_fault = min(origin + timedelta(minutes=f["start_min"]) for f in truth["faults"])
print(f"Data covers {cursor['next_min']:.0f} simulated minutes (until {data_until:%H:%M}).")
if data_until < first_fault:
    print(f"No injected fault has started yet: the first starts at {first_fault:%H:%M}. "
          "Run 01 again (or set minutes = 480) and rerun 02 before scoring.")

quarantined = spark.table(T("silver_quarantine")).select("event_id").distinct().count()
silver = spark.table(T("silver_telemetry")).count() + spark.table(T("silver_units")).count()
score["data_quality"] = {
    "injected_bad_records": dq_truth["bad_records"], "quarantined_records": quarantined,
    "injected_duplicates": dq_truth["duplicates"],
    "duplicates_removed": spark.table(T("bronze_events")).count() - silver - quarantined,
}
print(json.dumps(score, indent=2, default=str))

# COMMAND ----------

minutes = spark.table(T("gold_signal_minute")).toPandas()
display(evaluate.threshold_sweep(minutes, truth, data_until=data_until))
