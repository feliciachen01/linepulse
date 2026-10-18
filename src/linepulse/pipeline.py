"""Medallion pipeline: landing JSON -> bronze -> silver (+ quarantine) -> gold.

bronze  : raw events exactly as received, plus file + ingest metadata. Append only.
silver  : typed, deduplicated (MERGE on event_id), split by event type.
          Records failing data-quality rules go to silver_quarantine with the
          names of the rules they failed. Nothing is silently dropped.
gold    : business tables the API serves - per-minute signals with EWMA,
          drift alerts with affected lots, shift KPIs, line status, DQ summary.

Bronze and silver are incremental (Structured Streaming, Trigger.AvailableNow)
because that is the only trigger serverless compute supports. Gold is small
and recomputed in full each run, which keeps it simple and always consistent.
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame, functions as F, Window
from pyspark.sql.types import (BooleanType, DoubleType, StringType, StructField,
                               StructType, TimestampType, LongType)

from types import SimpleNamespace

from . import config as _config
from .store import TableStore

# Config as plain values, not a module object. Functions shipped to Spark workers
# (foreachBatch, applyInPandas) must not reference a module: on Databricks serverless
# a module imported from a Git folder carries a workspace loader that cannot be pickled.
C = SimpleNamespace(**{k: v for k, v in vars(_config).items() if k.isupper()})

# Ship this module's and store's functions by value, so workers never need to import
# the linepulse package (serverless, Spark Connect, local). Config is not registered:
# it travels as the plain namespace above.
try:
    import sys as _sys
    from pyspark import cloudpickle as _cp
    from . import store as _store_mod
    for _m in (_sys.modules[__name__], _store_mod):
        _cp.register_pickle_by_value(_m)
except Exception:  # pragma: no cover - older pyspark without the hook
    pass

RAW_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("plant_id", StringType()),
    StructField("line_id", StringType()),
    StructField("ts", StringType()),
    StructField("sensor", StringType()),
    StructField("value", DoubleType()),
    StructField("lot_id", StringType()),
    StructField("sku", StringType()),
    StructField("weight_g", DoubleType()),
    StructField("rejected", BooleanType()),
    StructField("reject_reason", StringType()),
])

BRONZE_SCHEMA = StructType(RAW_SCHEMA.fields + [
    StructField("_source_file", StringType()),
    StructField("_ingested_at", TimestampType()),
])

TELEMETRY_SCHEMA = StructType([
    StructField("event_id", StringType()), StructField("plant_id", StringType()),
    StructField("line_id", StringType()), StructField("ts", TimestampType()),
    StructField("sensor", StringType()), StructField("value", DoubleType()),
    StructField("_ingested_at", TimestampType()),
])
UNITS_SCHEMA = StructType([
    StructField("event_id", StringType()), StructField("plant_id", StringType()),
    StructField("line_id", StringType()), StructField("ts", TimestampType()),
    StructField("lot_id", StringType()), StructField("sku", StringType()),
    StructField("weight_g", DoubleType()), StructField("rejected", BooleanType()),
    StructField("reject_reason", StringType()), StructField("_ingested_at", TimestampType()),
])
QUARANTINE_SCHEMA = StructType([
    StructField("event_id", StringType()), StructField("event_type", StringType()),
    StructField("line_id", StringType()), StructField("ts_raw", StringType()),
    StructField("failed_rules", StringType()), StructField("raw_json", StringType()),
    StructField("_source_file", StringType()), StructField("_ingested_at", TimestampType()),
    StructField("batch_id", LongType()),
])


# ---------------------------------------------------------------- bronze

def run_bronze(store: TableStore, landing_path: str, use_autoloader: bool) -> None:
    spark = store.spark
    if use_autoloader:  # Databricks Auto Loader: tracks which files were already ingested
        reader = (spark.readStream.format("cloudFiles")
                  .option("cloudFiles.format", "json")
                  .option("cloudFiles.schemaLocation", store.checkpoint("bronze_schema"))
                  .option("rescuedDataColumn", "_rescued_data"))
    else:               # plain file source for the local harness
        reader = spark.readStream.format("json")
    df = (reader.schema(RAW_SCHEMA).option("pathGlobFilter", "*.jsonl").load(landing_path)
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_ingested_at", F.current_timestamp()))
    # On Databricks, fields that do not match RAW_SCHEMA land in _rescued_data instead of
    # being lost; silver quarantines those rows as schema drift (e.g. a legacy plant feed).
    store.stream_available_now(df.writeStream.outputMode("append"), "bronze_events")


# ---------------------------------------------------------------- silver

def _bounds_violation(sensor_col, value_col):
    cond = F.lit(False)
    for sensor, (lo, hi) in C.SENSOR_BOUNDS.items():
        cond = F.when(sensor_col == sensor, (value_col < lo) | (value_col > hi)).otherwise(cond)
    return cond


def dq_failures(df: DataFrame) -> DataFrame:
    """Add `failed` (array of rule names) and a typed `ts_parsed` column.

    Rules are deliberately explicit so they can be read by a QA lead.
    """
    ts = F.try_to_timestamp(F.col("ts"))  # ANSI mode: to_timestamp would throw on a bad string
    is_tel = F.col("event_type") == "telemetry"
    is_unit = F.col("event_type") == "unit"
    lo, hi = C.UNIT_WEIGHT_BOUNDS
    rules = [
        ("event_id_present", F.col("event_id").isNull()),
        ("event_type_known", ~F.col("event_type").isin("telemetry", "unit") | F.col("event_type").isNull()),
        ("ts_parseable", ts.isNull()),
        ("line_known", ~F.col("line_id").isin(*C.LINES) | F.col("line_id").isNull()),
        ("sensor_known", is_tel & (~F.col("sensor").isin(*C.SETPOINTS) | F.col("sensor").isNull())),
        ("value_present", is_tel & F.col("value").isNull()),
        ("value_in_physical_bounds", is_tel & F.coalesce(_bounds_violation(F.col("sensor"), F.col("value")), F.lit(False))),
        ("lot_present", is_unit & F.col("lot_id").isNull()),
        ("weight_in_bounds", is_unit & (F.col("weight_g").isNull() | (F.col("weight_g") < lo) | (F.col("weight_g") > hi))),
    ]
    if "_rescued_data" in df.columns:
        rules.append(("schema_drift", F.col("_rescued_data").isNotNull()))
    failed = F.filter(F.array(*[F.when(F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules]),
                      lambda x: x.isNotNull())
    return df.withColumn("ts_parsed", ts).withColumn("failed", failed)


def _silver_batch(store: TableStore):
    # Capture plain settings, not the SparkSession: on serverless (Spark Connect) the batch
    # function runs in a separate process and must use the batch's own session.
    cfg = dict(schema=store.schema, base_path=store.base_path,
               checkpoint_root=store.checkpoint_root, engine=store.engine)

    def process(batch: DataFrame, batch_id: int) -> None:
        store = TableStore(batch.sparkSession, **cfg)
        checked = dq_failures(batch).dropDuplicates(["event_id"])
        good = checked.filter(F.size("failed") == 0)

        tel = good.filter(F.col("event_type") == "telemetry").select(
            "event_id", "plant_id", "line_id", F.col("ts_parsed").alias("ts"), "sensor", "value", "_ingested_at")
        units = good.filter(F.col("event_type") == "unit").select(
            "event_id", "plant_id", "line_id", F.col("ts_parsed").alias("ts"), "lot_id", "sku",
            "weight_g", "rejected", "reject_reason", "_ingested_at")
        store.merge_insert_new(tel, "silver_telemetry")
        store.merge_insert_new(units, "silver_units")

        bad = checked.filter(F.size("failed") > 0).select(
            "event_id", "event_type", "line_id", F.col("ts").alias("ts_raw"),
            F.array_join("failed", ",").alias("failed_rules"),
            F.to_json(F.struct(*[c for c in RAW_SCHEMA.fieldNames()])).alias("raw_json"),
            "_source_file", "_ingested_at", F.lit(batch_id).cast("long").alias("batch_id"))
        if not bad.isEmpty():
            store.append(bad, "silver_quarantine")
    return process


def run_silver(store: TableStore) -> None:
    store.ensure("silver_telemetry", TELEMETRY_SCHEMA)
    store.ensure("silver_units", UNITS_SCHEMA)
    store.ensure("silver_quarantine", QUARANTINE_SCHEMA)
    stream = store.read_stream("bronze_events")
    store.foreach_batch_available_now(stream.writeStream.foreachBatch(_silver_batch(store)), "silver")


# ---------------------------------------------------------------- gold

SIGNAL_MINUTE_SCHEMA = StructType([
    StructField("line_id", StringType()), StructField("signal", StringType()),
    StructField("minute", TimestampType()), StructField("mean_value", DoubleType()),
    StructField("setpoint", DoubleType()), StructField("deviation", DoubleType()),
    StructField("n", LongType()), StructField("ewma_dev", DoubleType()),
    StructField("threshold", DoubleType()), StructField("over", BooleanType()),
])


def ewma_frame(pdf: pd.DataFrame, alpha: float, thresholds: dict) -> pd.DataFrame:
    """Per (line, signal) group: EWMA of deviation from setpoint, and the over-threshold flag.

    Pure pandas so it can run inside applyInPandas on Databricks and in the
    evaluation sweep locally.
    """
    pdf = pdf.sort_values("minute").copy()
    pdf["ewma_dev"] = pdf["deviation"].ewm(alpha=alpha, adjust=False).mean()
    thr = float(thresholds.get(pdf["signal"].iloc[0], float("inf")))
    pdf["threshold"] = thr
    pdf["over"] = pdf["ewma_dev"].abs() >= thr
    return pdf


def ewma_udf(alpha: float, thresholds: dict):
    """The function applyInPandas ships to workers: plain values only, no module references."""
    return lambda pdf: ewma_frame(pdf, alpha, thresholds)


def build_signal_minute(store: TableStore) -> DataFrame:
    tel = store.read("silver_telemetry")
    units = store.read("silver_units")
    setpoints = F.create_map(*[x for k, v in C.SETPOINTS.items() for x in (F.lit(k), F.lit(v))])
    ovens = (tel.filter(F.col("sensor").isin(*C.OVEN_SENSORS))
             .groupBy("line_id", F.col("sensor").alias("signal"), F.date_trunc("minute", "ts").alias("minute"))
             .agg(F.avg("value").alias("mean_value"), F.count("*").alias("n"))
             .withColumn("setpoint", setpoints[F.col("signal")]))
    fill = (units.groupBy("line_id", F.date_trunc("minute", "ts").alias("minute"))
            .agg(F.avg("weight_g").alias("mean_value"), F.count("*").alias("n"))
            .withColumn("signal", F.lit("fill_weight_g"))
            .withColumn("setpoint", F.lit(C.TARGET_WEIGHT_G)))
    minutes = (ovens.unionByName(fill)
               .withColumn("deviation", F.col("mean_value") - F.col("setpoint"))
               .select("line_id", "signal", "minute", "mean_value", "setpoint", "deviation", "n"))
    alpha, thresholds = C.DETECTOR["alpha"], C.DETECTOR["thresholds"]
    return minutes.groupBy("line_id", "signal").applyInPandas(
        ewma_udf(alpha, thresholds), schema=SIGNAL_MINUTE_SCHEMA)


def build_alerts(signal_minute: DataFrame, units: DataFrame) -> DataFrame:
    """Gaps-and-islands: consecutive over-threshold minutes become one alert."""
    k = C.DETECTOR["min_consecutive"]
    over = signal_minute.filter("over").withColumn("m_idx", (F.unix_timestamp("minute") / 60).cast("long"))
    w = Window.partitionBy("line_id", "signal").orderBy("m_idx")
    islands = over.withColumn("grp", F.col("m_idx") - F.row_number().over(w))
    last_minute = signal_minute.groupBy("line_id", "signal").agg(F.max("minute").alias("data_until"))
    runs = (islands.groupBy("line_id", "signal", "grp")
            .agg(F.min("minute").alias("start_minute"),
                 F.max("minute").alias("last_over_minute"),
                 F.count("*").alias("over_minutes"),
                 F.max(F.abs("ewma_dev")).alias("peak_abs_dev"),
                 F.avg("ewma_dev").alias("mean_dev"),
                 F.first("threshold").alias("threshold"))
            .filter(F.col("over_minutes") >= k)
            .join(last_minute, ["line_id", "signal"])
            .withColumn("detected_at", F.col("start_minute") + F.expr(f"INTERVAL {k} MINUTES"))
            .withColumn("end_minute", F.col("last_over_minute") + F.expr("INTERVAL 1 MINUTE"))
            .withColumn("status", F.when(F.col("last_over_minute") >= F.col("data_until"), "open").otherwise("closed"))
            .withColumn("direction", F.when(F.col("mean_dev") > 0, "high").otherwise("low"))
            .withColumn("alert_id", F.concat_ws("-", "line_id", "signal", F.date_format("start_minute", "yyyyMMdd'T'HHmm"))))
    # Which lots and how many packs were produced while the excursion lasted (recall scoping).
    impact = (runs.alias("a").join(units.alias("u"),
                                   (F.col("a.line_id") == F.col("u.line_id"))
                                   & (F.col("u.ts") >= F.col("a.start_minute"))
                                   & (F.col("u.ts") < F.col("a.end_minute")), "left")
              .groupBy("a.alert_id")
              .agg(F.array_sort(F.collect_set("u.lot_id")).alias("affected_lots"),
                   F.count("u.event_id").alias("units_in_window"),
                   F.sum(F.col("u.rejected").cast("int")).alias("rejects_in_window")))
    return (runs.join(impact, "alert_id", "left")
            .withColumn("duration_min", ((F.unix_timestamp("end_minute") - F.unix_timestamp("start_minute")) / 60).cast("int"))
            .select("alert_id", "line_id", "signal", "direction", "status", "start_minute", "detected_at",
                    "end_minute", "duration_min", F.round("peak_abs_dev", 2).alias("peak_abs_dev"), "threshold",
                    "affected_lots", "units_in_window",
                    F.coalesce("rejects_in_window", F.lit(0)).alias("rejects_in_window"))
            .orderBy("start_minute"))


def _shift_cols(ts_col):
    hour = F.hour(ts_col)
    shift = F.when((hour >= 6) & (hour < 14), "A").when((hour >= 14) & (hour < 22), "B").otherwise("C")
    shift_date = F.to_date(ts_col - F.expr("INTERVAL 6 HOURS"))
    return shift, shift_date


def build_shift_kpis(units: DataFrame, alerts: DataFrame) -> DataFrame:
    shift, shift_date = _shift_cols(F.col("ts"))
    u = units.withColumn("shift", shift).withColumn("shift_date", shift_date)
    accepted = ~F.col("rejected")
    over_label = F.when(accepted, F.col("weight_g") - C.LABEL_WEIGHT_G)
    kpis = (u.groupBy("line_id", "shift_date", "shift")
            .agg(F.count("*").alias("units"),
                 F.sum(F.col("rejected").cast("int")).alias("rejects"),
                 F.sum((F.col("reject_reason") == "underweight").cast("int")).alias("rejects_underweight"),
                 F.sum((F.col("reject_reason") == "bake_defect").cast("int")).alias("rejects_bake_defect"),
                 F.avg(F.when(accepted, F.col("weight_g"))).alias("avg_accepted_weight_g"),
                 F.avg(over_label).alias("giveaway_g_per_pack"),
                 (F.sum(over_label) / 1000).alias("giveaway_kg"),
                 F.min("ts").alias("first_ts"), F.max("ts").alias("last_ts"))
            .withColumn("scrap_rate_pct", F.round(100 * F.col("rejects") / F.col("units"), 3))
            .withColumn("giveaway_cost_cad", F.round(F.col("giveaway_kg") * C.GIVEAWAY_COST_CAD_PER_KG, 2)))
    a_shift, a_date = _shift_cols(F.col("start_minute"))
    alert_counts = (alerts.withColumn("shift", a_shift).withColumn("shift_date", a_date)
                    .groupBy("line_id", "shift_date", "shift").agg(F.count("*").alias("alerts")))
    return (kpis.join(alert_counts, ["line_id", "shift_date", "shift"], "left")
            .fillna({"alerts": 0})
            .select("line_id", "shift_date", "shift", "units", "rejects", "scrap_rate_pct",
                    "rejects_underweight", "rejects_bake_defect",
                    F.round("avg_accepted_weight_g", 2).alias("avg_accepted_weight_g"),
                    F.round("giveaway_g_per_pack", 3).alias("giveaway_g_per_pack"),
                    F.round("giveaway_kg", 2).alias("giveaway_kg"), "giveaway_cost_cad", "alerts",
                    "first_ts", "last_ts")
            .orderBy("line_id", "shift_date", "shift"))


def build_line_status(signal_minute: DataFrame, units: DataFrame, alerts: DataFrame, spark) -> DataFrame:
    as_of = signal_minute.groupBy("line_id").agg(F.max("minute").alias("as_of"))
    latest = (signal_minute.join(as_of, "line_id").filter(F.col("minute") == F.col("as_of"))
              .groupBy("line_id", "as_of").pivot("signal", C.OVEN_SENSORS + ["fill_weight_g"])
              .agg(F.round(F.first("mean_value"), 2)))
    recent = (units.join(as_of, "line_id")
              .filter(F.col("ts") >= F.col("as_of") - F.expr("INTERVAL 15 MINUTES"))
              .groupBy("line_id").agg(F.round(100 * F.avg(F.col("rejected").cast("double")), 2).alias("scrap_rate_15m_pct"),
                                      F.max("lot_id").alias("current_lot")))
    open_alerts = (alerts.filter(F.col("status") == "open").groupBy("line_id")
                   .agg(F.count("*").alias("open_alerts"), F.collect_list("alert_id").alias("open_alert_ids")))
    meta = spark.createDataFrame([(k, v["sku"], v["product"]) for k, v in C.LINES.items()], ["line_id", "sku", "product"])
    return (meta.join(latest, "line_id", "left").join(recent, "line_id", "left").join(open_alerts, "line_id", "left")
            .fillna({"open_alerts": 0})
            .withColumn("status", F.when(F.col("open_alerts") > 0, "ALERT")
                        .when(F.col("scrap_rate_15m_pct") > 2.0, "WARN").otherwise("OK"))
            .orderBy("line_id"))


def build_dq_summary(quarantine: DataFrame) -> DataFrame:
    return (quarantine.withColumn("rule", F.explode(F.split("failed_rules", ",")))
            .groupBy("event_type", "rule").agg(F.count("*").alias("records"))
            .orderBy(F.desc("records")))


def run_gold(store: TableStore) -> dict:
    spark = store.spark
    units = store.read("silver_units")
    sm = build_signal_minute(store)
    store.overwrite(sm, "gold_signal_minute")
    sm = store.read("gold_signal_minute")
    alerts = build_alerts(sm, units)
    store.overwrite(alerts, "gold_alerts")
    alerts = store.read("gold_alerts")
    store.overwrite(build_shift_kpis(units, alerts), "gold_shift_kpis")
    store.overwrite(build_line_status(sm, units, alerts, spark), "gold_line_status")
    store.overwrite(build_dq_summary(store.read("silver_quarantine")), "gold_dq_summary")
    return {t: store.read(t).count() for t in
            ["bronze_events", "silver_telemetry", "silver_units", "silver_quarantine",
             "gold_signal_minute", "gold_alerts", "gold_shift_kpis", "gold_line_status", "gold_dq_summary"]}


def run_all(store: TableStore, landing_path: str, use_autoloader: bool) -> dict:
    run_bronze(store, landing_path, use_autoloader)
    run_silver(store)
    return run_gold(store)
