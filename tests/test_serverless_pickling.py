"""Regression test for the Databricks serverless pickling error.

On serverless, modules imported from a Git folder carry a workspace import loader in
__spec__/__loader__ that cannot be pickled. Functions Spark ships to workers
(foreachBatch, applyInPandas) must never drag a module object along by value.
Here we make every linepulse module's loader unpicklable, as on Databricks, and check
that the shipped functions still serialize.
"""

import threading

import pytest
from pyspark import cloudpickle

from linepulse import config, pipeline, store as store_mod
from linepulse.store import TableStore


class _WorkspaceLoader:  # stand-in for dbruntime's loader: holds a lock, so it cannot be pickled
    def __init__(self):
        self.lock = threading.Lock()


@pytest.fixture
def unpicklable_loaders(monkeypatch):
    loader = _WorkspaceLoader()
    for mod in (config, pipeline, store_mod):  # every linepulse module, as on Databricks
        monkeypatch.setattr(mod, "__loader__", loader)
        monkeypatch.setattr(mod.__spec__, "loader", loader)
    with pytest.raises(Exception):
        cloudpickle.dumps(loader)  # sanity: the stand-in really is unpicklable


def test_foreach_batch_function_pickles(unpicklable_loaders, spark):
    store = TableStore.local(spark, "/tmp/linepulse-pickle-test", engine="parquet")
    fn = pipeline._silver_batch(store)
    cloudpickle.loads(cloudpickle.dumps(fn))


def test_apply_in_pandas_function_pickles(unpicklable_loaders):
    fn = pipeline.ewma_udf(pipeline.C.DETECTOR["alpha"], pipeline.C.DETECTOR["thresholds"])
    cloudpickle.loads(cloudpickle.dumps(fn))
