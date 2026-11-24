"""One place to build the SparkSession, tuned for a 16 GB laptop.

On Databricks the session already exists and `get()` just returns it; Delta is built in.
Locally we add Delta via delta-spark, keep the JVM small and put every temp file in the project.
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession

from solaratlas import config

_spark = None


def on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get() -> SparkSession:
    global _spark
    if _spark is not None:
        return _spark
    if on_databricks():
        _spark = SparkSession.builder.getOrCreate()
        # Serverless runs UDFs in sandboxes that don't have this project on their path, so ship our
        # functions inside the pickled UDF instead of as an import.
        from pyspark import cloudpickle

        import solaratlas
        cloudpickle.register_pickle_by_value(solaratlas)
        return _spark
    from delta import configure_spark_with_delta_pip

    tmp = config.ROOT / ".cache" / "spark"
    tmp.mkdir(parents=True, exist_ok=True)
    b = (
        SparkSession.builder.appName("solaratlas")
        .master(os.environ.get("SA_SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.environ.get("SA_SPARK_MEM", "2g"))
        .config("spark.sql.shuffle.partitions", "64")   # AQE coalesces small ones; 25M wide rows need room
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.memory.fraction", "0.7")              # more execution memory for wide joins
        .config("spark.sql.files.maxPartitionBytes", "64m")  # smaller tasks: less memory each
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", str(tmp))
        .config("spark.sql.warehouse.dir", str(tmp / "warehouse"))
        .config("spark.driver.extraJavaOptions",
                f"-Djava.io.tmpdir={tmp} -Dderby.system.home={tmp} -XX:+UseG1GC")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .config("spark.jars.ivy", str(config.ROOT / ".cache" / "ivy"))
    )
    _spark = configure_spark_with_delta_pip(b).getOrCreate()
    _spark.sparkContext.setLogLevel("WARN")
    return _spark


def stop():
    global _spark
    if _spark is not None and not on_databricks():
        _spark.stop()
    _spark = None


def keep(df, name: str):
    """Reuse a result several times: cache() locally, a small Delta table on Databricks.

    cache() is not available on serverless by default, and recomputing a seeded random split
    could change which rows it picks.
    """
    if not on_databricks():
        return df.cache()
    path = config.table_path("_tmp", name)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    return get().read.format("delta").load(path)


def release(df):
    if not on_databricks():
        df.unpersist()
