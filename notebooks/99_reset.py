# Databricks notebook source
# MAGIC %md
# MAGIC # LinePulse 99 - reset (deletes all LinePulse tables and files)
# MAGIC Use before re-running a clean demo. Leaves the schema and volume in place.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "linepulse")
dbutils.widgets.dropdown("confirm", "no", ["no", "yes"])
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
assert dbutils.widgets.get("confirm") == "yes", "Set confirm = yes to delete LinePulse data."

# COMMAND ----------

for t in ["bronze_events", "silver_telemetry", "silver_units", "silver_quarantine", "gold_signal_minute",
          "gold_alerts", "gold_quality_minute", "gold_shift_kpis", "gold_line_status", "gold_dq_summary"]:
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{t}")
for d in ["landing", "ground_truth", "_checkpoints"]:
    dbutils.fs.rm(f"/Volumes/{catalog}/{schema}/raw/{d}", recurse=True)
print("LinePulse data removed.")
