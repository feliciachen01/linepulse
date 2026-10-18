# Databricks notebook source
# MAGIC %md
# MAGIC # LinePulse 00 - one-time setup
# MAGIC Creates the schema and the volume that holds landing files and stream checkpoints.
# MAGIC Free Edition's default catalog is usually `workspace`; check Catalog Explorer if yours differs.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "linepulse")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'LinePulse: simulated bakery-line lakehouse'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw COMMENT 'Landing files, ground truth, checkpoints'")
display(spark.sql(f"SHOW VOLUMES IN {catalog}.{schema}"))
