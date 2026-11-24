"""Change-aware writes, so downstream incremental consumers see only real changes.

Comparing many columns inside a Delta MERGE over 25M rows is too heavy for a laptop (one attempt
ran 75 minutes without finishing). Instead each row carries `_row_hash` (xxhash64 of the compared
columns). Changed rows are found with a narrow (id, hash) anti-join, and only those, usually a tiny
set, are MERGEd. Unchanged rows are never rewritten, so they produce no Change Data Feed entries.
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from solaratlas import config
from solaratlas.lakehouse.spark import get


def row_hash(cols: list[str]):
    # coalesce so a NULL and a missing value hash the same way on both sides
    return F.xxhash64(*[F.coalesce(F.col(c).cast("string"), F.lit("\u0000")) for c in cols])


def upsert_changed(df: DataFrame, path: str, region_key: str, compare: list[str]) -> dict:
    spark = get()
    src = df.withColumn("_row_hash", row_hash(compare))
    if not DeltaTable.isDeltaTable(spark, path):
        src.write.format("delta").partitionBy("region").save(path)
        spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        return {"mode": "create"}
    props = spark.sql(f"SHOW TBLPROPERTIES delta.`{path}`").collect()
    if not any(r.key == "delta.enableChangeDataFeed" and r.value == "true" for r in props):
        spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    tgt_df = spark.read.format("delta").load(path).where(F.col("region") == region_key)
    if "_row_hash" not in tgt_df.columns or tgt_df.limit(1).count() == 0:
        # first load of this region, or a table from before hashing: one plain partition write
        (src.write.format("delta").mode("overwrite").option("mergeSchema", "true")
         .option("replaceWhere", f"region = '{region_key}'").save(path))
        return {"mode": "rewrite"}
    # the source query is expensive (joins, UDFs): materialise once, then compare narrow columns
    tmp = config.table_path("_tmp", "upsert_" + path.rstrip("/").split("/")[-1] + "_" + region_key)
    src.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tmp)
    src = spark.read.format("delta").load(tmp)
    tgt = tgt_df.select("id", "_row_hash")
    changed = src.join(tgt, ["id", "_row_hash"], "left_anti")           # new or different
    gone = tgt.select("id").join(src.select("id"), "id", "left_anti")    # no longer present
    changed_tmp = tmp + "_changed"
    changed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(changed_tmp)
    changed = spark.read.format("delta").load(changed_tmp)
    n_changed, n_gone = changed.count(), gone.count()
    t = DeltaTable.forPath(spark, path)
    if n_changed:
        (t.alias("t").merge(changed.alias("s"), f"t.id = s.id AND t.region = '{region_key}'")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    if n_gone:
        (t.alias("t").merge(gone.alias("g"), f"t.id = g.id AND t.region = '{region_key}'")
         .whenMatchedDelete().execute())
    print(f"[upsert] {path.split('/')[-1]}: {n_changed:,} changed, {n_gone:,} removed")
    return {"mode": "merge", "changed": n_changed, "removed": n_gone}


def latest_version(path: str) -> int:
    return DeltaTable.forPath(get(), path).history(1).select("version").first()[0]
