"""Silver: validated, deduplicated, enriched entities.

* buildings: geometry parsed once in an Arrow pandas UDF (area, perimeter, shape metrics,
  centroid), then assigned to state / district / locality / land use by a broadcast STRtree
  join. Buildings outside the region polygon are counted, not kept. Rule failures go to
  `silver.buildings_quarantine` with their reasons, and clean rows are MERGEd on the stable Overture
  GERS id, so a new release updates changed buildings and deletes demolished ones (region-scoped).
* places: POIs mapped to category groups (commercial / industrial / institutional / ...).
* cell_climate: POWER JSON flattened to one row per (cell, month) with specific PV yield.
"""
from __future__ import annotations

import h3
import numpy as np
import pandas as pd
import shapely
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.functions import pandas_udf
from pyspark.sql.window import Window

from solaratlas import config
from solaratlas.ingest.irradiance import STEP
from solaratlas.lakehouse import spatial
from solaratlas.lakehouse.spark import get
from solaratlas.model.solar import DAYS, MONTHS

M_PER_DEG_LAT = 110_574.0
M_PER_DEG_LON_EQ = 111_320.0

GEOM_SCHEMA = T.StructType([
    T.StructField("footprint_m2", T.DoubleType()),
    T.StructField("perimeter_m", T.DoubleType()),
    T.StructField("compactness", T.DoubleType()),
    T.StructField("elongation", T.DoubleType()),
    T.StructField("n_vertices", T.IntegerType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("geom_valid", T.BooleanType()),
    T.StructField("geom_type", T.StringType()),
])


def footprint_metrics(wkb: pd.Series) -> pd.DataFrame:
    """Area, perimeter and shape metrics in metres via a local equirectangular projection.

    Scaling x by cos(lat) is linear, so for a building (tens of metres) the projection error is
    far below 0.1%. Shape metrics feed the building-type classifier:
      compactness = 4*pi*A / P^2  (1 = circle; long sheds and row houses are low)
      elongation  = long side / short side of the minimum rotated rectangle
    """
    geoms = shapely.from_wkb(wkb.to_numpy(), on_invalid="ignore")
    ok = ~shapely.is_missing(geoms)
    cent = shapely.centroid(geoms)
    lon = shapely.get_x(cent)
    lat = shapely.get_y(cent)
    kx = np.cos(np.radians(np.nan_to_num(lat))) * M_PER_DEG_LON_EQ
    area = shapely.area(geoms) * kx * M_PER_DEG_LAT
    # perimeter and rotated rectangle need real metric coordinates: transform per geometry
    perim = np.full(len(geoms), np.nan)
    elong = np.full(len(geoms), np.nan)
    idx = np.flatnonzero(ok)
    if len(idx):
        counts = shapely.get_num_coordinates(geoms[idx])
        scale = np.repeat(kx[idx], counts)

        def to_m(c):
            return np.column_stack([c[:, 0] * scale, c[:, 1] * M_PER_DEG_LAT])

        gm = shapely.transform(geoms[idx], to_m)
        perim[idx] = shapely.length(shapely.boundary(gm))
        rect = shapely.oriented_envelope(gm)
        coords, owner = shapely.get_coordinates(rect, return_index=True)
        starts = np.unique(owner, return_index=True)[1]
        n = np.diff(np.append(starts, len(owner)))
        e = np.full(len(idx), np.nan)
        good = n >= 4                                   # a real rectangle (degenerate -> line)
        s0 = starts[good]
        a = np.hypot(*(coords[s0 + 1] - coords[s0]).T)
        b = np.hypot(*(coords[s0 + 2] - coords[s0 + 1]).T)
        with np.errstate(divide="ignore", invalid="ignore"):
            e[np.unique(owner)[good]] = np.maximum(a, b) / np.minimum(a, b)
        elong[idx] = e
    with np.errstate(divide="ignore", invalid="ignore"):
        compact = 4 * np.pi * area / perim ** 2
    return pd.DataFrame({
        "footprint_m2": np.where(ok, area, np.nan),
        "perimeter_m": perim,
        "compactness": compact,
        "elongation": elong,
        "n_vertices": np.where(ok, shapely.get_num_coordinates(geoms), 0).astype("int32"),
        "lon": lon,
        "lat": lat,
        "geom_valid": ok & shapely.is_valid(geoms),
        "geom_type": shapely.get_type_id(geoms).astype(str),
    })


footprint_udf = pandas_udf(footprint_metrics, returnType=GEOM_SCHEMA)


@pandas_udf(T.StructType([T.StructField("h3_r9", T.StringType()),
                          T.StructField("h3_r10", T.StringType()),
                          T.StructField("h3_r11", T.StringType())]))
def h3_cells(lat: pd.Series, lon: pd.Series) -> pd.DataFrame:
    r11 = [h3.latlng_to_cell(a, o, 11) if a == a else None for a, o in zip(lat, lon, strict=True)]
    return pd.DataFrame({
        "h3_r9": [h3.cell_to_parent(c, 9) if c else None for c in r11],
        "h3_r10": [h3.cell_to_parent(c, 10) if c else None for c in r11],
        "h3_r11": r11,
    })


def cell_expr(lon_col: str, lat_col: str):
    """Nearest lattice point; same rule as ingest.irradiance.cell_id()."""
    lon = F.format_number(F.round(F.col(lon_col) / STEP) * STEP, 2)
    lat = F.format_number(F.round(F.col(lat_col) / STEP) * STEP, 2)
    return F.concat_ws("_", lon, lat)


OSM_TYPE = {  # Overture building subtype -> our 4 classes (training labels)
    "residential": "residential", "commercial": "commercial", "industrial": "industrial",
    "education": "institutional", "medical": "institutional", "religious": "institutional",
    "civic": "institutional",
}


def buildings(region_key: str, run_id: str) -> dict:
    spark = get()
    a = config.load("assumptions")["roof"]
    release = config.load("sources")["overture_buildings"]["release"]
    reg = config.region(region_key)
    state_code = config.load("regions")["regions"][region_key].get("state_code", "IN-TN")
    bronze = (spark.read.format("delta").load(config.table_path("bronze", "buildings"))
              .where((F.col("_region") == region_key) & (F.col("_release") == release)))

    g = bronze.withColumn("g", footprint_udf("geometry_wkb")).select("*", "g.*").drop("g")
    g = g.withColumn("adm", spatial.admin_udf(state_code)("lon", "lat")).select("*", "adm.*").drop("adm")
    xmin, ymin, xmax, ymax = reg.bbox
    in_region = F.col("lon").between(xmin, xmax) & F.col("lat").between(ymin, ymax) & F.col("in_state")
    g = g.withColumn("in_region", F.coalesce(in_region, F.lit(False)))
    reason = F.array_compact(F.array(
        F.when(F.col("id").isNull(), F.lit("null_id")),
        F.when(~F.col("geom_valid"), F.lit("invalid_geometry")),
        F.when(~F.col("geom_type").isin("3", "6"), F.lit("not_polygon")),
        F.when(F.col("footprint_m2") < 2, F.lit("area_below_2m2")),
        F.when(F.col("footprint_m2") > a["max_footprint_m2"], F.lit("area_above_max")),
        F.when(F.col("in_region") & F.col("district").isNull(), F.lit("no_district")),
        F.when((F.col("height").isNotNull()) & ((F.col("height") <= 0) | (F.col("height") > 300)),
               F.lit("implausible_height")),
    ))
    w = Window.partitionBy("id").orderBy(F.col("source_confidence").desc_nulls_last())
    checked = (g.withColumn("_rn", F.row_number().over(w))
                .withColumn("dq_reasons", F.when(F.col("_rn") > 1,
                            F.array_union(reason, F.array(F.lit("duplicate_id")))).otherwise(reason)))
    # materialise once: the UDFs are the expensive part and three outputs read this
    tmp = config.table_path("_tmp", f"silver_checked_{region_key}")
    checked.write.format("delta").mode("overwrite").save(tmp)
    checked = spark.read.format("delta").load(tmp)

    inside = checked.where("in_region")
    outside = checked.where("NOT in_region").count()
    quarantine = (inside.where(F.size("dq_reasons") > 0)
                  .select("id", "_region", "_release", "dq_reasons", "footprint_m2", "lon", "lat",
                          "district", "source_dataset", "_source_file", F.lit(run_id).alias("_run_id"),
                          F.current_timestamp().alias("_quarantined_at")))
    (quarantine.write.format("delta").mode("overwrite").option("mergeSchema", "true")
       .option("replaceWhere", f"_region = '{region_key}'").partitionBy("_region")
       .save(config.table_path("silver", "buildings_quarantine")))

    label = F.coalesce(*[F.when(F.col("subtype") == k, F.lit(v)) for k, v in OSM_TYPE.items()])
    clean = (inside.where(F.size("dq_reasons") == 0)
             .withColumn("h", h3_cells("lat", "lon")).select("*", "h.*").drop("h")
             .select(
                 "id", F.col("_region").alias("region"), F.col("_release").alias("release"),
                 "geometry_wkb", "lon", "lat", F.round("footprint_m2", 2).alias("footprint_m2"),
                 F.round("perimeter_m", 2).alias("perimeter_m"), "compactness", "elongation",
                 "n_vertices", "height", "num_floors", "roof_shape", "class", "subtype",
                 label.alias("osm_type"), "source_dataset", "source_confidence",
                 "district", "district_id", "locality", "locality_id", "landuse",
                 "h3_r9", "h3_r10", "h3_r11", cell_expr("lon", "lat").alias("cell_id"),
                 F.current_timestamp().alias("_updated_at"), F.lit(run_id).alias("_run_id"),
             ))
    path = config.table_path("silver", "buildings")
    if not DeltaTable.isDeltaTable(spark, path):
        clean.limit(0).write.format("delta").partitionBy("region").save(path)
        spark.sql(f"ALTER TABLE delta.`{path}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    tgt = DeltaTable.forPath(spark, path)
    n_target = spark.read.format("delta").load(path).where(F.col("region") == region_key).limit(1).count()
    if n_target == 0:
        # first load of a region: a plain partition write is far cheaper than a MERGE join
        (clean.write.format("delta").mode("overwrite").option("mergeSchema", "true")
         .option("replaceWhere", f"region = '{region_key}'").save(path))
    else:
        (tgt.alias("t")
            .merge(clean.alias("s"), f"t.id = s.id AND t.region = '{region_key}'")
            # content changes only: a new Overture release re-stamps every row, so comparing
            # `release` would mark all 25M buildings changed. `release` therefore records the
            # release in which a building last actually changed.
            .whenMatchedUpdateAll(condition="NOT (t.geometry_wkb <=> s.geometry_wkb) "
                                            "OR NOT (t.height <=> s.height) "
                                            "OR NOT (t.num_floors <=> s.num_floors) "
                                            "OR NOT (t.subtype <=> s.subtype) "
                                            "OR NOT (t.district <=> s.district) "
                                            "OR NOT (t.locality <=> s.locality) "
                                            "OR NOT (t.landuse <=> s.landuse)")
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete(condition=f"t.region = '{region_key}'")
            .execute())
    stats = {
        "bronze_rows": checked.count(),
        "outside_region": outside,
        "quarantined": quarantine.count(),
        "silver_rows": spark.read.format("delta").load(path).where(F.col("region") == region_key).count(),
    }
    return stats


PLACE_GROUPS = [  # first match wins; matched against category + basic_category + hierarchy
    ("industrial", ["manufactur", "factory", "industrial", "warehouse", "wholesale", "textile_mill",
                    "foundry", "engineering", "printing", "mill"]),
    ("institutional", ["school", "college", "university", "education", "hospital", "clinic",
                       "health", "medical", "religio", "temple", "church", "mosque", "government",
                       "police", "post_office", "library", "community_center", "cultural_and_historic",
                       "community_and_government"]),
    ("commercial", ["store", "shop", "retail", "restaurant", "eatery", "food", "hotel", "lodg",
                    "bank", "financ", "office", "service", "real_estate", "market", "mall",
                    "automotive", "beauty", "salon", "pharmacy", "cafe", "bar", "event", "travel"]),
]


def places(region_key: str, run_id: str) -> int:
    spark = get()
    reg = config.load("regions")["regions"][region_key]
    release = config.load("sources")["overture_buildings"]["release"]
    f = (config.LANDING / "places" / f"release={release}" / f"state={reg.get('state_code', 'IN-TN')}"
         / "places.parquet")
    if not f.exists():
        print("[silver.places] no places landed; skipping")
        return 0
    text = F.lower(F.concat_ws(" ", F.coalesce("category", F.lit("")),
                               F.coalesce("basic_category", F.lit("")),
                               F.concat_ws(" ", F.coalesce("hierarchy", F.array()))))
    grp = None
    for name, words in PLACE_GROUPS:
        cond = F.lit(False)
        for wd in words:
            cond = cond | text.contains(wd)
        grp = F.when(cond, F.lit(name)) if grp is None else grp.when(cond, F.lit(name))
    xmin, ymin, xmax, ymax = config.region(region_key).bbox
    df = (spark.read.parquet(str(f))
          .where(F.col("lon").between(xmin, xmax) & F.col("lat").between(ymin, ymax))
          .withColumn("place_group", grp.otherwise(F.lit("other")))
          .withColumn("h", h3_cells("lat", "lon")).select("*", "h.*").drop("h")
          .withColumn("region", F.lit(region_key)).withColumn("_run_id", F.lit(run_id)))
    (df.write.format("delta").mode("overwrite").option("mergeSchema", "true")
       .partitionBy("region").option("replaceWhere", f"region = '{region_key}'")
       .save(config.table_path("silver", "places")))
    return spark.read.format("delta").load(config.table_path("silver", "places")) \
        .where(F.col("region") == region_key).count()


def cell_climate(run_id: str) -> int:
    """One row per (cell, month): GHI, temperature and specific yield (kWh/kWp)."""
    spark = get()
    pv = config.load("assumptions")["pv"]
    raw = spark.read.format("delta").load(config.table_path("bronze", "irradiance"))
    month_map = F.create_map(*[x for m in MONTHS for x in (F.lit(m), F.lit(float(DAYS[m])))])
    ghi = F.from_json(F.get_json_object("payload", "$.properties.parameter.ALLSKY_SFC_SW_DWN"),
                      "map<string,double>")
    t2m = F.from_json(F.get_json_object("payload", "$.properties.parameter.T2M"),
                      "map<string,double>")
    df = (raw.select("lon", "lat", ghi.alias("ghi"), t2m.alias("t2m"))
          .select("lon", "lat", "ghi", "t2m", F.explode(F.map_keys("ghi")).alias("month"))
          .where(F.col("month") != "ANN")
          .select(
              cell_expr("lon", "lat").alias("cell_id"), "lon", "lat", "month",
              F.col("ghi")[F.col("month")].alias("ghi_kwh_m2_day"),
              F.col("t2m")[F.col("month")].alias("t2m_c"),
              month_map[F.col("month")].alias("days"))
          .withColumn("specific_yield_kwh_kwp",
                      F.col("ghi_kwh_m2_day") * F.col("days") * F.lit(pv["performance_ratio"])
                      * (1 + F.lit(pv["temp_coeff_per_c"])
                         * (F.col("t2m_c") + F.lit(pv["cell_temp_rise_c"]) - 25)))
          .withColumn("_run_id", F.lit(run_id)))
    df.write.format("delta").mode("overwrite").save(config.table_path("silver", "cell_climate"))
    return df.count()


def cell_hourly(run_id: str) -> int:
    """One row per (cell, hour of the year): PV output per kWp. Spark twin of model.hourly.pv_per_kwp."""
    spark = get()
    path = config.table_path("bronze", "irradiance_hourly")
    if not DeltaTable.isDeltaTable(spark, path):
        return 0
    pv = config.load("assumptions")["pv"]
    k = config.load("metering")["cell_temp_coeff_c_per_wm2"]
    raw = spark.read.format("delta").load(path)
    ghi = F.from_json(F.get_json_object("payload", "$.ALLSKY_SFC_SW_DWN"), "map<string,double>")
    t2m = F.from_json(F.get_json_object("payload", "$.T2M"), "map<string,double>")
    g = F.greatest(F.col("ghi"), F.lit(0.0))
    t = F.when(F.col("t2m") < -50, 25.0).otherwise(F.col("t2m"))
    df = (raw.select("lon", "lat", "year", ghi.alias("gm"), t2m.alias("tm"))
          .select("lon", "lat", "year", "tm", F.explode("gm").alias("ts", "ghi"))
          .withColumn("t2m", F.col("tm")[F.col("ts")])
          .select(cell_expr("lon", "lat").alias("cell_id"), "year", "ts",
                  F.substring("ts", 5, 2).cast("int").alias("month"),
                  F.substring("ts", 9, 2).cast("int").alias("hour"),
                  (g / 1000 * pv["performance_ratio"]
                   * (1 + F.lit(pv["temp_coeff_per_c"]) * (t + k * g - 25))).alias("pv_kwh_per_kwp"))
          .withColumn("_run_id", F.lit(run_id)))
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
        .save(config.table_path("silver", "cell_hourly"))
    return spark.read.format("delta").load(config.table_path("silver", "cell_hourly")).count()
