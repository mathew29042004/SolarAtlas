"""Gold: business-ready tables for the API, analysts and policy scenarios.

  gold.building_solar        one row per building: district, type, roof, shading, kWh, economics
  gold.district_summary      districts ranked: potential, viable roofs, investment, subsidy
  gold.locality_summary      towns/villages ranked within each district
  gold.hex_summary           H3 res-7 (state view) and res-8 (city view) aggregates
  gold.household_economics   per climate cell x bimonthly consumption: best size, payback
  gold.scenario_district     policy what-ifs (config/scenarios.yaml) per district
  gold.region_summary        headline numbers
  gold.dim_tariff            SCD Type 2 tariff versions

Economics differ by building type. Homes get PM Surya Ghar, telescopic slabs and a size chosen
for their (estimated) consumption. Shops, factories and institutions get no subsidy, offset a
flat energy rate and self-consume most of their output.
"""
from __future__ import annotations

import h3
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from solaratlas import config
from solaratlas.lakehouse.spark import get
from solaratlas.model import solar
from solaratlas.model import tariff as tariffs

PROFILES = list(range(100, 2050, 50))   # bimonthly units a household might use
NONDOMESTIC = ["commercial", "industrial", "institutional"]
LOOKUP_KEYS = ["cell_id", "est_units", "cap_bucket", "shade_bucket"]


def _write(df: DataFrame, name: str, region_key: str | None = None):
    w = df.write.format("delta").mode("overwrite")
    if region_key is not None:   # partition-scoped overwrite: schema may only grow
        w = w.option("mergeSchema", "true").partitionBy("region") \
             .option("replaceWhere", f"region = '{region_key}'")
    else:
        w = w.option("overwriteSchema", "true")
    w.save(config.table_path("gold", name))


def _read(layer: str, name: str) -> DataFrame:
    return get().read.format("delta").load(config.table_path(layer, name))


def usable_fraction_expr(col: str):
    bands = config.load("assumptions")["roof"]["usable_fraction_by_size"]
    expr = None
    for b in bands:
        cond = F.col(col) <= F.lit(float(b["max_m2"]))
        expr = F.when(cond, F.lit(b["fraction"])) if expr is None else expr.when(cond, b["fraction"])
    return expr.otherwise(F.lit(bands[-1]["fraction"]))


def shading_expr():
    """Spark twin of solar.shading_loss (parity-tested)."""
    s = config.load("assumptions")["shading"]
    loss = (F.greatest(F.lit(0.0), F.col("built_frac_ring") - s["free_below_built_fraction"])
            * s["loss_per_unit_fraction"])
    loss = F.when(F.col("footprint_m2") >= s["large_roof_m2"], loss / 2).otherwise(loss)
    return F.least(F.coalesce(loss, F.lit(0.0)), F.lit(float(s["max_loss"])))


def units_expr():
    """Spark twin of solar.estimated_units."""
    r = config.load("assumptions")["residential"]
    u = F.least(F.greatest(F.col("footprint_m2") * r["units_per_m2_bimonthly"], F.lit(r["min_units"])),
                F.lit(r["max_units"]))
    return (F.round(u / 50) * 50).cast("int")


def cell_yield() -> DataFrame:
    return (_read("silver", "cell_climate").groupBy("cell_id")
            .agg(F.sum("specific_yield_kwh_kwp").alias("annual_yield_kwh_kwp"),
                 F.avg("ghi_kwh_m2_day").alias("mean_ghi_kwh_m2_day")))


def base(region_key: str, ids: DataFrame | None = None) -> DataFrame:
    """Physical potential per building (scenario-independent).

    With `ids`, every input is narrowed to those buildings *before* the joins, so an incremental
    run joins thousands of rows, not millions.
    """
    a = config.load("assumptions")
    b = _read("silver", "buildings").where(F.col("region") == region_key)
    t = _read("silver", "building_type").where(F.col("region") == region_key).drop("region", "_run_id")
    f = (_read("silver", "building_features").where(F.col("region") == region_key)
         .select("id", "built_frac_ring"))
    if ids is not None:      # caller guarantees the id set is small (see FULL_REBUILD_SHARE)
        ids = F.broadcast(ids.select("id"))
        b, t, f = (x.join(ids, "id", "left_semi") for x in (b, t, f))
    cap = float(a["economics"]["residential_cap_kwp"])
    return (b.join(t, "id", "left").join(f, "id", "left")
            .join(F.broadcast(cell_yield()), "cell_id", "left")
            .withColumn("building_type", F.coalesce("building_type", F.lit("residential")))
            .withColumn("usable_fraction", usable_fraction_expr("footprint_m2"))
            .withColumn("eligible", F.col("footprint_m2") >= a["roof"]["min_footprint_m2"])
            .withColumn("usable_roof_m2", F.when(F.col("eligible"),
                        F.col("footprint_m2") * F.col("usable_fraction")).otherwise(0.0))
            .withColumn("roof_kwp", F.col("usable_roof_m2") / a["roof"]["m2_per_kwp"])
            .withColumn("shading_loss", shading_expr())
            .withColumn("shade_bucket", F.round(F.round(F.col("shading_loss") / 0.05) * 0.05, 2))
            .withColumn("eff_yield", F.col("annual_yield_kwh_kwp") * (1 - F.col("shading_loss")))
            .withColumn("annual_kwh", F.col("roof_kwp") * F.col("eff_yield"))
            .withColumn("co2_t_per_year", F.col("annual_kwh") / 1000 * a["grid"]["co2_t_per_mwh"])
            .withColumn("est_units", F.when(F.col("building_type") == "residential", units_expr()))
            .withColumn("cap_bucket", F.when(F.col("roof_kwp") >= 1,
                        F.least(F.floor(F.col("roof_kwp") * 2) / 2, F.lit(cap))))
            .withColumn("size_class",
                        F.when(~F.col("eligible"), "too_small")
                         .when(F.col("roof_kwp") < 3, "small_home")
                         .when(F.col("roof_kwp") < 10, "home")
                         .when(F.col("roof_kwp") < 100, "commercial")
                         .otherwise("industrial")))


def residential_lookup(keys: list[tuple], scenario: str = "baseline") -> DataFrame:
    """Best home system per (cell, units, roof cap, shade) under the active config/overrides.

    A few thousand distinct keys, computed on the driver with the same Python model the API
    uses, then broadcast-joined: no Python UDF over millions of rows.
    """
    tv = tariffs.as_of()
    yields = {r.cell_id: r.annual_yield_kwh_kwp for r in cell_yield().collect()}
    rows = []
    for cell, units, capb, shade in keys:
        y = yields.get(cell)
        if y is None or units is None or capb is None:
            continue
        best, _ = solar.best_size(y * (1 - shade), units, tv, max_kwp=capb)
        rows.append((scenario, cell, int(units), float(capb), float(shade), float(best["kwp"]),
                     float(best["annual_saving_inr"]), float(best["capex_inr"]),
                     float(best["subsidy_inr"]), float(best["net_cost_inr"]), best["payback_years"],
                     float(best["lifetime_saving_inr"]), float(best["bill_before_inr"])))
    return get().createDataFrame(rows, (
        "scenario string, cell_id string, est_units int, cap_bucket double, shade_bucket double, "
        "system_kwp double, annual_saving_inr double, capex_inr double, subsidy_inr double, "
        "net_cost_inr double, payback_years double, lifetime_saving_inr double, bill_before_inr double"))


def nondomestic_cols(df: DataFrame) -> DataFrame:
    """Economics for commercial / industrial / institutional rows, as Spark expressions
    (twin of solar.nondomestic_economics)."""
    cats = config.load("nondomestic")["categories"]
    om = config.load("assumptions")["economics"]["om_inr_per_kwp_year"]

    def pick(field):
        e = None
        for c in NONDOMESTIC:
            v = float(cats[c][field])
            cond = F.col("building_type") == c
            e = F.when(cond, F.lit(v)) if e is None else e.when(cond, v)
        return e

    kwp = F.least(F.col("roof_kwp"), pick("cap_kwp"))
    saving = kwp * F.col("eff_yield") * pick("self_consumption") * pick("energy_rate_inr") - kwp * om
    return (df.withColumn("nd_kwp", kwp).withColumn("nd_saving", saving)
              .withColumn("nd_capex", kwp * pick("capex_inr_per_kwp")))


def _watermark(consumer: str) -> int | None:
    path = config.table_path("ops", "watermarks")
    spark = get()
    if not DeltaTable.isDeltaTable(spark, path):
        return None
    r = spark.read.format("delta").load(path).where(F.col("consumer") == consumer) \
        .agg(F.max("version")).first()[0]
    return None if r is None else int(r)


def _set_watermark(consumer: str, version: int, run_id: str):
    get().createDataFrame([(consumer, version, run_id)], "consumer string, version long, run_id string") \
        .withColumn("at", F.current_timestamp()).write.format("delta").mode("append") \
        .save(config.table_path("ops", "watermarks"))


FULL_REBUILD_SHARE = 0.2   # above this share of changed buildings, a full rebuild is cheaper
MAX_BROADCAST_IDS = 2_000_000
SOURCES = [("silver", "buildings"), ("silver", "building_type"), ("silver", "building_features")]


def _changed_ids(region_key: str, since: int, layer: str = "silver", name: str = "buildings"):
    """Ids changed in a source table after version `since`, from its Change Data Feed.

    Returns (id, _change_type) with the latest change per id; 'delete' only matters for
    silver.buildings (a building that no longer exists).
    """
    cdf = (get().read.format("delta").option("readChangeFeed", "true").option("startingVersion", since + 1)
           .load(config.table_path(layer, name)).where(F.col("region") == region_key))
    w = Window.partitionBy("id").orderBy(F.desc("_commit_version"),
                                         F.when(F.col("_change_type") == "delete", 0).otherwise(1).desc())
    return (cdf.where(F.col("_change_type") != "update_preimage")
            .withColumn("rn", F.row_number().over(w)).where("rn = 1").select("id", "_change_type"))


def _materialise(df: DataFrame, name: str) -> DataFrame:
    """Write an expensive intermediate once (Delta in _tmp) and read it back: the wide joins run
    once, and later actions (distinct keys, final write) read a plain table."""
    path = config.table_path("_tmp", name)
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
    return get().read.format("delta").load(path)


def _solar_rows(b: DataFrame, run_id: str) -> DataFrame:
    keys = [tuple(r) for r in b.where("building_type = 'residential' AND cap_bucket IS NOT NULL")
            .select(*LOOKUP_KEYS).distinct().collect()]
    look = residential_lookup(keys).drop("scenario")
    viable = float(config.load("scenarios")["viable_payback_years"])
    j = nondomestic_cols(b.join(F.broadcast(look), LOOKUP_KEYS, "left"))
    res = F.col("building_type") == "residential"
    return (j
            .withColumn("system_kwp", F.when(res, F.col("system_kwp")).otherwise(F.col("nd_kwp")))
            .withColumn("annual_saving_inr", F.when(res, F.col("annual_saving_inr")).otherwise(F.col("nd_saving")))
            .withColumn("capex_inr", F.when(res, F.col("capex_inr")).otherwise(F.col("nd_capex")))
            .withColumn("subsidy_inr", F.when(res, F.col("subsidy_inr")).otherwise(F.lit(0.0)))
            .withColumn("net_cost_inr", F.col("capex_inr") - F.coalesce("subsidy_inr", F.lit(0.0)))
            .withColumn("payback_years", F.when(F.col("annual_saving_inr") > 0,
                        F.round(F.col("net_cost_inr") / F.col("annual_saving_inr"), 1)))
            .withColumn("viable", F.coalesce(F.col("payback_years") <= viable, F.lit(False)))
            .select("id", "region", "release", "district", "locality", "lon", "lat", "geometry_wkb",
                    "h3_r9", "h3_r10", "cell_id", "footprint_m2", "height", "source_dataset",
                    "building_type", "type_source", "type_prob", "landuse",
                    F.round("usable_roof_m2", 1).alias("usable_roof_m2"),
                    F.round("roof_kwp", 2).alias("roof_kwp"),
                    F.round("shading_loss", 3).alias("shading_loss"), "shade_bucket", "cap_bucket",
                    F.round("eff_yield", 1).alias("eff_yield"),
                    F.round("annual_kwh", 0).alias("annual_kwh"),
                    F.round("co2_t_per_year", 3).alias("co2_t_per_year"),
                    "est_units", F.round("system_kwp", 2).alias("system_kwp"),
                    F.round("annual_saving_inr", 0).alias("annual_saving_inr"),
                    F.round("capex_inr", 0).alias("capex_inr"),
                    F.round("subsidy_inr", 0).alias("subsidy_inr"),
                    F.round("net_cost_inr", 0).alias("net_cost_inr"), "payback_years", "viable",
                    "size_class", "eligible", "annual_yield_kwh_kwp", F.lit(run_id).alias("_run_id")))


def building_solar(region_key: str, run_id: str, incremental: bool = True) -> dict:
    """Full rebuild on first run; afterwards only buildings whose inputs changed.

    Inputs are silver.buildings, silver.building_type and silver.building_features, all with
    Change Data Feed and change-aware writes. Gold keeps one watermark per input
    (ops.watermarks) and recomputes the union of changed ids.
    """
    from solaratlas.lakehouse.delta_utils import latest_version
    spark = get()
    gold_path = config.table_path("gold", "building_solar")
    versions = {f"{lay}.{n}": latest_version(config.table_path(lay, n)) for lay, n in SOURCES}
    marks = {k: (_watermark(f"gold.building_solar:{region_key}:{k}") if incremental else None)
             for k in versions}
    has_gold = DeltaTable.isDeltaTable(spark, gold_path) and \
        spark.read.format("delta").load(gold_path).where(F.col("region") == region_key).limit(1).count() > 0

    def save_marks():
        for k, v in versions.items():
            _set_watermark(f"gold.building_solar:{region_key}:{k}", v, run_id)

    if not has_gold or any(m is None for m in marks.values()):
        b = _materialise(base(region_key), f"gold_base_{region_key}")
        _write(_solar_rows(b, run_id), "building_solar", region_key)
        save_marks()
        return {"mode": "full", "changed": None, "rows": _count(gold_path, region_key)}
    if all(marks[k] >= versions[k] for k in versions):
        print("[gold] inputs unchanged since last run; building_solar is up to date")
        return {"mode": "noop", "changed": 0, "rows": _count(gold_path, region_key)}
    parts = []
    for lay, n in SOURCES:
        k = f"{lay}.{n}"
        if marks[k] < versions[k]:
            c = _changed_ids(region_key, marks[k], lay, n)
            if n != "buildings":   # a feature/type change never deletes a building
                c = c.withColumn("_change_type", F.lit("update"))
            parts.append(c)
    allc = parts[0]
    for p in parts[1:]:
        allc = allc.unionByName(p)
    # a delete in silver.buildings wins over any other change for that id
    changes = (allc.groupBy("id").agg(F.max(F.when(F.col("_change_type") == "delete", 1).otherwise(0)).alias("d"))
               .cache())
    n_changed = changes.count()
    total = _count(gold_path, region_key)
    if n_changed > FULL_REBUILD_SHARE * total or n_changed > MAX_BROADCAST_IDS:
        changes.unpersist()
        print(f"[gold] {n_changed:,} of {total:,} changed (over {FULL_REBUILD_SHARE:.0%} or {MAX_BROADCAST_IDS:,} ids); full rebuild instead")
        b = _materialise(base(region_key), f"gold_base_{region_key}")
        _write(_solar_rows(b, run_id), "building_solar", region_key)
        save_marks()
        return {"mode": "full_fallback", "changed": n_changed, "rows": _count(gold_path, region_key)}
    upserts = changes.where("d = 0").select("id")
    deletes = changes.where("d = 1").select("id")
    b = _materialise(base(region_key, upserts), f"gold_base_incr_{region_key}")
    rows = _solar_rows(b, run_id)
    tgt = DeltaTable.forPath(spark, gold_path)
    (tgt.alias("t").merge(rows.alias("s"), f"t.id = s.id AND t.region = '{region_key}'")
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    if deletes.limit(1).count():
        (tgt.alias("t").merge(deletes.alias("d"), f"t.id = d.id AND t.region = '{region_key}'")
            .whenMatchedDelete().execute())
    changes.unpersist()
    save_marks()
    print(f"[gold] incremental: {n_changed} changed building(s)")
    return {"mode": "incremental", "changed": n_changed, "rows": _count(gold_path, region_key)}


def _count(path: str, region_key: str) -> int:
    return get().read.format("delta").load(path).where(F.col("region") == region_key).count()


def _area_aggs():
    res = F.col("building_type") == "residential"
    return [
        F.count("*").alias("buildings"),
        F.sum(F.col("eligible").cast("int")).alias("eligible_buildings"),
        F.round(F.sum("footprint_m2") / 1e6, 3).alias("roof_km2"),
        F.round(F.sum("roof_kwp") / 1000, 2).alias("potential_mwp"),
        F.round(F.sum("annual_kwh") / 1e6, 2).alias("potential_gwh_year"),
        F.round(F.sum("co2_t_per_year") / 1000, 2).alias("co2_kt_per_year"),
        F.sum(F.col("viable").cast("int")).alias("viable_buildings"),
        F.round(F.sum(F.when(F.col("viable"), F.col("system_kwp"))) / 1000, 2).alias("viable_mwp"),
        F.round(F.sum(F.when(F.col("viable"), F.col("capex_inr"))) / 1e7, 1).alias("viable_investment_cr"),
        F.round(F.sum(F.when(F.col("viable"), F.col("subsidy_inr"))) / 1e7, 1).alias("viable_subsidy_cr"),
        F.round(F.sum(F.when(F.col("viable"), F.col("annual_saving_inr"))) / 1e7, 1).alias("viable_saving_cr_year"),
        F.sum(res.cast("int")).alias("homes"),
        F.sum((res & F.col("viable")).cast("int")).alias("viable_homes"),
        F.round(F.sum(F.when(~res, F.col("roof_kwp"))) / 1000, 2).alias("nondomestic_mwp"),
        F.round(F.avg("shading_loss"), 4).alias("mean_shading_loss"),
        F.round(F.avg("annual_yield_kwh_kwp"), 0).alias("mean_yield_kwh_kwp"),
        F.round(F.expr("percentile_approx(payback_years, 0.5)"), 1).alias("median_payback_years"),
    ]


def area_summaries(region_key: str, run_id: str) -> dict:
    bs = _read("gold", "building_solar").where(F.col("region") == region_key)
    d = (bs.where(F.col("district").isNotNull()).groupBy("region", "district")
         .agg(*_area_aggs(), F.avg("lon").alias("lon"), F.avg("lat").alias("lat"))
         .withColumn("rank_potential",
                     F.dense_rank().over(Window.partitionBy("region").orderBy(F.desc("potential_gwh_year"))))
         .withColumn("_run_id", F.lit(run_id)))
    _write(d, "district_summary", region_key)
    loc = (bs.where(F.col("locality").isNotNull()).groupBy("region", "district", "locality")
           .agg(*_area_aggs(), F.avg("lon").alias("lon"), F.avg("lat").alias("lat"))
           .withColumn("rank_in_district", F.dense_rank().over(
               Window.partitionBy("region", "district").orderBy(F.desc("potential_gwh_year"))))
           .withColumn("_run_id", F.lit(run_id)))
    _write(loc, "locality_summary", region_key)
    parent = F.udf(lambda c, r: h3.cell_to_parent(c, r) if c else None, T.StringType())
    hexes = None
    for res in (7, 8):
        hx = (bs.withColumn("h3", parent("h3_r9", F.lit(res)))
              .groupBy("region", "h3").agg(
                  F.count("*").alias("buildings"),
                  F.round(F.sum("roof_kwp") / 1000, 3).alias("potential_mwp"),
                  F.round(F.sum("annual_kwh") / 1e6, 3).alias("potential_gwh_year"),
                  F.round(F.sum("co2_t_per_year"), 1).alias("co2_t_per_year"),
                  F.sum(F.col("viable").cast("int")).alias("viable_buildings"),
                  F.first("district", ignorenulls=True).alias("district"))
              .withColumn("res", F.lit(res)))
        hexes = hx if hexes is None else hexes.unionByName(hx)
    _write(hexes.withColumn("_run_id", F.lit(run_id)), "hex_summary", region_key)
    return {"districts": d.count(), "localities": loc.count()}


def scenarios(region_key: str, run_id: str) -> int:
    """Policy what-ifs per district.

    Millions of homes collapse to a few thousand groups first (same district, cell,
    consumption, roof cap and shade give identical economics), so each scenario only re-prices
    those groups: aggregate before join. Non-domestic roofs are fewer and re-priced per row.
    """
    spark = get()
    b = _read("gold", "building_solar").where(F.col("region") == region_key)
    res_groups = (b.where("building_type = 'residential' AND cap_bucket IS NOT NULL AND district IS NOT NULL")
                  .groupBy("district", *LOOKUP_KEYS).agg(F.count("*").alias("n")).cache())
    nd_base = (b.where("building_type != 'residential' AND eligible AND district IS NOT NULL")
               .select("district", "building_type", "roof_kwp", "eff_yield").cache())
    keys = [tuple(r) for r in res_groups.select(*LOOKUP_KEYS).distinct().collect()]
    cfg = config.load("scenarios")
    viable = float(cfg["viable_payback_years"])
    parts = []
    for key, sc in cfg["scenarios"].items():
        with config.overrides(sc["overrides"] or {}):
            look = residential_lookup(keys, key)
            ok = F.coalesce(F.col("payback_years") <= viable, F.lit(False))
            r = (res_groups.join(F.broadcast(look), LOOKUP_KEYS).withColumn("ok", ok)
                 .groupBy("district").agg(
                     F.sum("n").alias("homes"),
                     F.sum(F.when(F.col("ok"), F.col("n")).otherwise(0)).alias("viable_homes"),
                     F.sum(F.when(F.col("ok"), F.col("n") * F.col("system_kwp"))).alias("home_kwp"),
                     F.sum(F.when(F.col("ok"), F.col("n") * F.col("subsidy_inr"))).alias("subsidy_inr"),
                     F.sum(F.when(F.col("ok"), F.col("n") * F.col("capex_inr"))).alias("home_capex")))
            nd = nondomestic_cols(nd_base)
            nd_ok = (F.col("nd_saving") > 0) & (F.col("nd_capex") / F.col("nd_saving") <= viable)
            nd = (nd.withColumn("ok", nd_ok).groupBy("district").agg(
                F.sum(F.col("ok").cast("int")).alias("viable_nondomestic"),
                F.sum(F.when(F.col("ok"), F.col("nd_kwp"))).alias("nd_kwp"),
                F.sum(F.when(F.col("ok"), F.col("nd_capex"))).alias("nd_capex")))
            z = F.lit(0.0)
            s = (r.join(nd, "district", "full")
                 .select(F.lit(key).alias("scenario"), F.lit(sc["label"]).alias("label"), "district",
                         F.coalesce("homes", F.lit(0)).alias("homes"),
                         F.coalesce("viable_homes", F.lit(0)).alias("viable_homes"),
                         F.coalesce("viable_nondomestic", F.lit(0)).alias("viable_nondomestic"),
                         F.round((F.coalesce("home_kwp", z) + F.coalesce("nd_kwp", z)) / 1000, 2).alias("viable_mwp"),
                         F.round(F.coalesce("subsidy_inr", z) / 1e7, 1).alias("subsidy_cr"),
                         F.round((F.coalesce("home_capex", z) + F.coalesce("nd_capex", z)) / 1e7, 1)
                          .alias("investment_cr")))
            # collect while the overrides are active (Spark plans lazily; nd prices are literals)
            parts.extend(s.collect())
    out = (spark.createDataFrame(parts).withColumn("region", F.lit(region_key))
           .withColumn("_run_id", F.lit(run_id)))
    _write(out, "scenario_district", region_key)
    res_groups.unpersist()
    nd_base.unpersist()
    return len(parts)


def household_economics(run_id: str) -> int:
    """Best system size per (cell, consumption profile), roof unconstrained, no shade."""
    tv = tariffs.as_of()
    rows = []
    for r in cell_yield().collect():
        for units in PROFILES:
            best, _ = solar.best_size(r.annual_yield_kwh_kwp, units, tv)
            rows.append((r.cell_id, units, float(best["kwp"]), float(best["annual_saving_inr"]),
                         float(best["net_cost_inr"]), float(best["subsidy_inr"]), best["payback_years"],
                         float(best["lifetime_saving_inr"]), float(best["bill_before_inr"]),
                         best["lifetime_saving_inr"] > 0, tv.version))
    df = get().createDataFrame(rows, (
        "cell_id string, bimonthly_units int, best_kwp double, annual_saving_inr double, "
        "net_cost_inr double, subsidy_inr double, payback_years double, "
        "lifetime_saving_inr double, bill_before_inr double, worth_it boolean, tariff_version string"))
    _write(df.withColumn("_run_id", F.lit(run_id)), "household_economics")
    return df.count()


def dim_tariff(run_id: str) -> int:
    rows = []
    for t in tariffs.versions("tn_domestic"):
        for band, slabs in (("low", t.low), ("high", t.high)):
            lower = 0
            for upto, rate in slabs:
                rows.append((t.scheme, t.version, t.effective_from, t.effective_to, band,
                             float(lower), None if upto is None else float(upto), float(rate),
                             t.free_units, float(t.switch_at_units), t.verified, t.source))
                lower = upto or lower
    df = get().createDataFrame(rows, (
        "scheme string, version string, effective_from date, effective_to date, band string, "
        "from_units double, to_units double, rate_inr double, free_units int, "
        "switch_at_units double, verified boolean, source string"))
    df = df.withColumn("is_current", F.col("effective_to").isNull()).withColumn("_run_id", F.lit(run_id))
    _write(df, "dim_tariff")
    return df.count()


def region_summary(region_key: str, run_id: str) -> dict:
    """Headline numbers, built as a Spark DataFrame end to end (types come from the aggregation,
    so empty or NULL values never break schema inference)."""
    reg = config.region(region_key)
    b = _read("gold", "building_solar").where(F.col("region") == region_key)
    q = _read("silver", "buildings_quarantine").where(F.col("_region") == region_key).count()
    types = ["residential", "commercial", "industrial", "institutional"]
    counts = b.groupBy().pivot("building_type", types).count()
    counts = counts.select(*[F.coalesce(F.col(t), F.lit(0)).cast("long").alias(f"type_{t}") for t in types])
    df = (b.agg(*_area_aggs(),
                F.countDistinct("district").alias("districts"))
          .crossJoin(counts)
          .withColumn("region", F.lit(region_key)).withColumn("name", F.lit(reg.name))
          .withColumn("state", F.lit(reg.state)).withColumn("quarantined", F.lit(q).cast("long"))
          .withColumn("release", F.lit(config.load("sources")["overture_buildings"]["release"]))
          .withColumn("tariff_version", F.lit(tariffs.as_of().version))
          .withColumn("_run_id", F.lit(run_id)))
    (df.write.format("delta").mode("overwrite").option("mergeSchema", "true")
       .partitionBy("region").option("replaceWhere", f"region = '{region_key}'")
       .save(config.table_path("gold", "region_summary")))
    return df.first().asDict()
