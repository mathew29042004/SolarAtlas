"""Publish Gold to serving: Delta -> Parquet snapshot -> DuckDB file, swapped in atomically.

The API never reads the lake directly. It reads a read-only DuckDB file built from a snapshot
that already passed the quality gate, so a bad run can never reach users, and the previous
snapshot stays available for an instant rollback (`current` is just a symlink).
"""
from __future__ import annotations

import os
import shutil

import duckdb

from solaratlas import config
from solaratlas.lakehouse.spark import get

TABLES = ["building_solar", "hex_summary", "household_economics", "region_summary", "dim_tariff",
          "district_summary", "locality_summary", "scenario_district"]
OPS = [("ops", "pipeline_runs"), ("ops", "dq_results"), ("ops", "model_metrics"),
       ("silver", "cell_hourly")]
KEEP = 2


def publish(run_id: str) -> str:
    spark = get()
    snap = config.SERVING / "snapshots" / run_id
    snap.mkdir(parents=True, exist_ok=True)
    for t in TABLES:
        df = spark.read.format("delta").load(config.table_path("gold", t))
        if t != "building_solar":      # small tables: one file; buildings keep parallel files
            df = df.coalesce(1)
        df.write.mode("overwrite").parquet(str(snap / t))
    db = snap / "solaratlas.duckdb"
    con = duckdb.connect(str(db))
    for t in TABLES:
        con.execute(f"CREATE OR REPLACE TABLE {t} AS SELECT * FROM read_parquet('{snap / t}/*.parquet')")
    con.execute("CREATE INDEX IF NOT EXISTS ix_b_id ON building_solar(id)")
    for layer, t in OPS:  # small operational tables for the ops page
        p = config.LAKE / layer / t
        if (p / "_delta_log").exists():
            spark.read.format("delta").load(str(p)).coalesce(1).write.mode("overwrite").parquet(str(snap / t))
            con.execute(f"CREATE OR REPLACE TABLE {t} AS SELECT * FROM read_parquet('{snap / t}/*.parquet')")
            shutil.rmtree(snap / t)
    con.execute("CREATE TABLE IF NOT EXISTS _meta AS SELECT ? AS run_id, now() AS published_at", [run_id])
    con.close()
    for t in TABLES:  # the DuckDB file is self-contained; drop the parquet copies
        shutil.rmtree(snap / t)
    link = config.SERVING / "current"
    tmp = config.SERVING / ".current.tmp"
    if tmp.is_symlink() or tmp.exists():
        tmp.unlink()
    os.symlink(snap.relative_to(config.SERVING), tmp)
    os.replace(tmp, link)  # atomic swap
    snaps = sorted((config.SERVING / "snapshots").iterdir())
    for old in snaps[:-KEEP]:
        shutil.rmtree(old)
    return str(db)
