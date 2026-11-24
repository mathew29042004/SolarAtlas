"""Bronze: landing files -> Delta, unchanged except for lineage columns.

Each (release, region) is one partition and is written with `replaceWhere`, so a rerun
replaces exactly that partition: idempotent, no duplicates, and other regions are untouched.
"""
from __future__ import annotations

import json

from pyspark.sql import functions as F

from solaratlas import config
from solaratlas.ingest.buildings import landing_dir
from solaratlas.lakehouse.spark import get


def buildings(region_key: str, run_id: str) -> int:
    spark = get()
    release = config.load("sources")["overture_buildings"]["release"]
    root = landing_dir(release, region_key)
    if config.load("regions")["regions"][region_key].get("tiled"):
        # only tiles whose manifest exists are complete (written atomically after the data)
        files = sorted(str(m.parent / "buildings.parquet") for m in root.glob("tile=*/_manifest.json"))
    else:
        files = [str(root / "buildings.parquet")] if (root / "buildings.parquet").exists() else []
    if not files:
        raise FileNotFoundError(f"nothing landed under {root}; run ingest first")
    print(f"[bronze] {len(files)} landing file(s)")
    df = (
        spark.read.parquet(*files)
        .withColumn("_release", F.lit(release))
        .withColumn("_region", F.lit(region_key))
        .withColumn("_source_file", F.regexp_replace(F.col("_metadata.file_path"), "^.*/landing/", "landing/"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(run_id))
    )
    (df.write.format("delta").mode("overwrite")
       .partitionBy("_region", "_release")
       .option("replaceWhere", f"_region = '{region_key}' AND _release = '{release}'")
       .save(config.table_path("bronze", "buildings")))
    return df.count()


def irradiance(run_id: str) -> int:
    """Raw POWER JSON, one row per lattice point; payload kept verbatim as a string."""
    spark = get()
    rows = []
    for f in sorted((config.LANDING / "irradiance" / "nasa_power_climatology").glob("*.json")):
        body = json.loads(f.read_text())
        rows.append((body["_request"]["lon"], body["_request"]["lat"], f.read_text(),
                     body["_fetched_at"], str(f.relative_to(config.LANDING.parent))))
    df = (spark.createDataFrame(rows, "lon double, lat double, payload string, "
                                       "fetched_at string, _source_file string")
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_run_id", F.lit(run_id)))
    df.write.format("delta").mode("overwrite").save(config.table_path("bronze", "irradiance"))
    return len(rows)


def irradiance_hourly(run_id: str) -> int:
    """Raw POWER hourly JSON (one row per lattice point and year), payload kept verbatim."""
    spark = get()
    year = config.load("metering")["year"]
    d = config.LANDING / "irradiance" / f"nasa_power_hourly_{year}"
    rows = []
    for f in sorted(d.glob("cell=*.json")):
        body = json.loads(f.read_text())
        rows.append((body["_request"]["lon"], body["_request"]["lat"], year, json.dumps(body["parameter"]),
                     body["_fetched_at"], str(f.relative_to(config.LANDING.parent))))
    if not rows:
        return 0
    df = (spark.createDataFrame(rows, "lon double, lat double, year int, payload string, "
                                       "fetched_at string, _source_file string")
          .withColumn("_ingested_at", F.current_timestamp()).withColumn("_run_id", F.lit(run_id)))
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .save(config.table_path("bronze", "irradiance_hourly"))
    return len(rows)
