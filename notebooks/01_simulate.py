# Databricks notebook source
# MAGIC %md
# MAGIC # LinePulse 01 - simulate the next chunk of line data
# MAGIC Appends `minutes` of events to the landing folder and continues from the last run
# MAGIC (cursor in `ground_truth/cursor.json`). Run it on a schedule to imitate a live line.

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../src"))  # the repo's src/ folder, from a Git folder

from datetime import datetime
from linepulse import simulator

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "linepulse")
dbutils.widgets.text("minutes", "60")
dbutils.widgets.text("origin", "2026-10-19T06:00:00")

catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
root = f"/Volumes/{catalog}/{schema}/raw"

# COMMAND ----------

summary = simulator.write_chunk(root, datetime.fromisoformat(dbutils.widgets.get("origin")),
                                float(dbutils.widgets.get("minutes")))
print(summary)
