"""Where tables live: Unity Catalog on Databricks, plain folders locally.

The pipeline code only talks to a TableStore, so the same transforms run in a
Databricks job and in the local test harness.

Local engines:
  delta   (default) - same table format as Databricks; needs the Delta jar,
            which Spark downloads from Maven on first run.
  parquet (fallback) - for machines that cannot reach Maven. MERGE is emulated
            with an anti-join + append, which is fine for single-writer tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


@dataclass
class TableStore:
    spark: SparkSession
    schema: str | None = None      # e.g. "workspace.linepulse" on Databricks
    base_path: str | None = None   # e.g. "./local_data/tables" locally
    checkpoint_root: str = ""
    engine: str = "delta"

    @classmethod
    def unity_catalog(cls, spark, catalog: str, schema: str, volume: str = "raw"):
        return cls(spark, schema=f"{catalog}.{schema}",
                   checkpoint_root=f"/Volumes/{catalog}/{schema}/{volume}/_checkpoints")

    @classmethod
    def local(cls, spark, root: str, engine: str = "delta"):
        root = str(Path(root).resolve())
        return cls(spark, base_path=f"{root}/tables", checkpoint_root=f"{root}/_checkpoints", engine=engine)

    @property
    def is_local(self) -> bool:
        return self.base_path is not None

    def path(self, name: str) -> str:
        return f"{self.base_path}/{name}"

    def ref(self, name: str) -> str:
        """A name usable in spark.table() and SQL."""
        return f"{self.engine}.`{self.path(name)}`" if self.is_local else f"{self.schema}.{name}"

    def checkpoint(self, name: str) -> str:
        return f"{self.checkpoint_root}/{name}"

    def exists(self, name: str) -> bool:
        if not self.is_local:
            return self.spark.catalog.tableExists(self.ref(name))
        if self.engine == "delta":
            from delta.tables import DeltaTable
            return DeltaTable.isDeltaTable(self.spark, self.path(name))
        return any(Path(self.path(name)).glob("*.parquet"))

    def ensure(self, name: str, schema: StructType) -> None:
        if not self.exists(name):
            self.overwrite(self.spark.createDataFrame([], schema), name)

    def read(self, name: str) -> DataFrame:
        if self.is_local:
            return self.spark.read.format(self.engine).load(self.path(name))
        return self.spark.read.table(self.ref(name))

    def read_stream(self, name: str) -> DataFrame:
        if not self.is_local:
            return self.spark.readStream.table(self.ref(name))
        if self.engine == "delta":
            return self.spark.readStream.format("delta").load(self.path(name))
        schema = self.spark.read.parquet(self.path(name)).schema
        return self.spark.readStream.schema(schema).parquet(self.path(name))

    def merge_insert_new(self, df: DataFrame, name: str, key: str = "event_id") -> None:
        """Idempotent insert: a key already in the table is never written twice."""
        if self.is_local and self.engine == "parquet":
            existing = self.read(name).select(key)
            self.append(df.join(existing, key, "left_anti"), name)
            return
        from delta.tables import DeltaTable
        target = (DeltaTable.forPath(self.spark, self.path(name)) if self.is_local
                  else DeltaTable.forName(self.spark, self.ref(name)))
        (target.alias("t").merge(df.alias("s"), f"t.{key} = s.{key}")
         .whenNotMatchedInsertAll().execute())

    def overwrite(self, df: DataFrame, name: str) -> None:
        w = df.write.format(self.engine if self.is_local else "delta").mode("overwrite")
        if not self.is_local or self.engine == "delta":
            w = w.option("overwriteSchema", "true")
        w.save(self.path(name)) if self.is_local else w.saveAsTable(self.ref(name))

    def append(self, df: DataFrame, name: str) -> None:
        w = df.write.format(self.engine if self.is_local else "delta").mode("append")
        w.save(self.path(name)) if self.is_local else w.saveAsTable(self.ref(name))

    def stream_available_now(self, writer, name: str):
        """Run an incremental (Trigger.AvailableNow) stream into a table and wait for it."""
        writer = (writer.format(self.engine if self.is_local else "delta")
                  .option("checkpointLocation", self.checkpoint(name)).trigger(availableNow=True))
        q = writer.start(self.path(name)) if self.is_local else writer.toTable(self.ref(name))
        q.awaitTermination()
        return q

    def foreach_batch_available_now(self, writer, checkpoint_name: str):
        """Run a foreachBatch stream incrementally; the batch function does its own writes."""
        q = (writer.option("checkpointLocation", self.checkpoint(checkpoint_name))
             .trigger(availableNow=True).start())
        q.awaitTermination()
        return q
