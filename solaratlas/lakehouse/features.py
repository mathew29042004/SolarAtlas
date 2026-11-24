"""Neighbourhood features per building, built from H3 cells (equality joins, no geometry).

  density   built-up share in the building's res-11 cell and its 6 neighbours (~15,000 m2),
            building count and mean size in the res-9 cell (~0.1 km2)
  places    commercial / industrial / institutional POIs in the same res-11 cell and within
            the res-10 cell + ring (~100 m)
  shape     area, perimeter ratio, compactness, elongation, vertex count (from silver)
  land use  zone the centroid falls in (from silver)

These drive the building-type classifier and the shading proxy.
"""
from __future__ import annotations

import h3
from pyspark.sql import functions as F
from pyspark.sql import types as T

from solaratlas import config
from solaratlas.lakehouse.delta_utils import upsert_changed
from solaratlas.lakehouse.spark import get

R11_AREA_M2 = h3.average_hexagon_area(11, unit="m^2")
GROUPS = ["commercial", "industrial", "institutional"]

ring1 = F.udf(lambda c: list(h3.grid_disk(c, 1)) if c else [], T.ArrayType(T.StringType()))


def build(region_key: str, run_id: str) -> int:
    spark = get()
    b = (spark.read.format("delta").load(config.table_path("silver", "buildings"))
         .where(F.col("region") == region_key)
         .select("id", "region", "h3_r9", "h3_r10", "h3_r11", "footprint_m2", "perimeter_m",
                 "compactness", "elongation", "n_vertices", "landuse", "osm_type", "district"))

    r11 = b.groupBy("h3_r11").agg(F.sum("footprint_m2").alias("built_m2"), F.count("*").alias("n"))
    ring = (r11.select(F.explode(ring1("h3_r11")).alias("center"), "built_m2", "n")
            .groupBy("center").agg(F.sum("built_m2").alias("ring_built_m2"), F.sum("n").alias("ring_n"))
            .withColumnRenamed("center", "h3_r11"))
    r9 = b.groupBy("h3_r9").agg(F.count("*").alias("r9_buildings"),
                                F.avg("footprint_m2").alias("r9_mean_area"))

    from delta.tables import DeltaTable
    ppath = config.table_path("silver", "places")
    if DeltaTable.isDeltaTable(spark, ppath):
        p = spark.read.format("delta").load(ppath).where(F.col("region") == region_key)
    else:  # no places for this region: POI features become zero
        p = spark.createDataFrame([], "h3_r10 string, h3_r11 string, place_group string")
    p11 = p.groupBy("h3_r11").pivot("place_group", GROUPS).count().na.fill(0)
    p11 = p11.select("h3_r11", *[F.col(g).alias(f"poi11_{g}") for g in GROUPS])
    p10 = p.groupBy("h3_r10").pivot("place_group", GROUPS).count().na.fill(0)
    p10ring = (p10.select(F.explode(ring1("h3_r10")).alias("center"), *GROUPS)
               .groupBy("center").agg(*[F.sum(g).alias(f"poi10r_{g}") for g in GROUPS])
               .withColumnRenamed("center", "h3_r10"))

    f = (b.join(ring, "h3_r11", "left").join(r9, "h3_r9", "left")
         .join(p11, "h3_r11", "left").join(p10ring, "h3_r10", "left")
         .na.fill(0, [f"poi11_{g}" for g in GROUPS] + [f"poi10r_{g}" for g in GROUPS])
         .withColumn("built_frac_ring",
                     F.least(F.lit(1.0), F.col("ring_built_m2") / F.lit(7 * R11_AREA_M2)))
         .withColumn("log_area", F.log1p("footprint_m2"))
         .withColumn("perim_ratio", F.col("perimeter_m") / F.sqrt("footprint_m2"))
         .withColumn("landuse_zone", F.coalesce(F.split("landuse", ":")[0], F.lit("none")))
         .withColumn("landuse_class", F.coalesce(F.split("landuse", ":")[1], F.lit("")))
         .withColumn("_run_id", F.lit(run_id))
         .drop("landuse"))
    # float sums/averages depend on Spark's summation order and differ in the last bits between
    # runs; round them so an unchanged neighbourhood compares equal and produces no CDF rows
    f = f.select(*[F.round(c, 4).alias(c) if t == "double" else F.col(c) for c, t in f.dtypes])
    # change-aware write: only buildings whose features moved show up in the Change Data Feed
    compare = [c for c in f.columns if c not in ("id", "region", "_run_id")]
    upsert_changed(f, config.table_path("silver", "building_features"), region_key, compare)
    return spark.read.format("delta").load(config.table_path("silver", "building_features")) \
        .where(F.col("region") == region_key).count()
