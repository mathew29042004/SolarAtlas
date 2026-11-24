"""Pipeline runner: a small DAG of tasks with a run log in ops.pipeline_runs.

    python -m solaratlas.pipeline run --region coimbatore
    python -m solaratlas.pipeline run --region coimbatore --from silver   # resume from a stage

Locally this replaces Airflow (saves ~1.5 GB of RAM); on Databricks the same task functions
are the steps of a Job (see databricks/job.yml). Every task is idempotent, so any task can be
retried or the whole run replayed.
"""
from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from datetime import UTC, datetime

from solaratlas import config
from solaratlas.ingest import buildings as ing_buildings
from solaratlas.ingest import irradiance as ing_irradiance
from solaratlas.lakehouse import bronze, classify, features, gold, quality, silver
from solaratlas.lakehouse.spark import get, stop

STAGES = ["ingest", "bronze", "silver", "features", "classify", "gold", "quality", "publish"]


def run(region_key: str, start: str = "ingest", skip_publish: bool = False,
        full_refresh: bool = False) -> dict:
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
    timings, status, error = {}, "success", None
    stats = {"driver_mem": os.environ.get("SA_SPARK_MEM", "2g"), "start": start, "full_refresh": full_refresh}
    todo = STAGES[STAGES.index(start):]
    if skip_publish:
        todo = [s for s in todo if s != "publish"]
    print(f"run {run_id} region={region_key} stages={todo}")
    t_run = time.time()
    try:
        for stage in todo:
            t0 = time.time()
            print(f"> {stage}")
            if stage == "ingest":
                reg_cfg = config.load("regions")["regions"][region_key]
                if reg_cfg.get("tiled"):
                    from solaratlas.ingest import context, divisions, statewide
                    divisions.extract(reg_cfg["state_code"])
                    stats["landing"] = statewide.extract(region_key)["rows"]
                    context.extract("places", region_key)
                    context.extract("land_use", region_key)
                else:
                    stats["landing"] = ing_buildings.extract(region_key)["rows"]
                ing_irradiance.fetch(region_key)
                ing_irradiance.fetch_hourly(region_key, config.load("metering")["year"])
            elif stage == "bronze":
                stats["bronze_buildings"] = bronze.buildings(region_key, run_id)
                stats["bronze_irradiance"] = bronze.irradiance(run_id)
                stats["bronze_irradiance_hourly"] = bronze.irradiance_hourly(run_id)
            elif stage == "silver":
                stats.update(silver.buildings(region_key, run_id))
                stats["places"] = silver.places(region_key, run_id)
                stats["climate_rows"] = silver.cell_climate(run_id)
                stats["hourly_rows"] = silver.cell_hourly(run_id)
            elif stage == "features":
                stats["features"] = features.build(region_key, run_id)
            elif stage == "classify":
                m = classify.train_and_predict(region_key, run_id)
                stats["classifier"] = {k: m.get(k) for k in ("accuracy", "macro_f1", "labels", "predicted")}
            elif stage == "gold":
                stats["gold_buildings"] = gold.building_solar(region_key, run_id, not full_refresh)
                stats.update(gold.area_summaries(region_key, run_id))
                stats["scenario_rows"] = gold.scenarios(region_key, run_id)
                stats["economics_rows"] = gold.household_economics(run_id)
                stats["tariff_rows"] = gold.dim_tariff(run_id)
                stats["summary"] = gold.region_summary(region_key, run_id)
            elif stage == "quality":
                if "bronze_rows" not in stats:  # resumed after silver: recount from tables
                    stats.update(_recount(region_key))
                quality.run(region_key, run_id, stats)
            elif stage == "publish":
                from solaratlas.serve.publish import publish
                stats["serving_db"] = publish(run_id)
            timings[stage] = round(time.time() - t0, 1)
            print(f"  {stage} done in {timings[stage]} s")
    except Exception as e:  # recorded, then re-raised so the exit code is non-zero
        status, error = "failed", f"{type(e).__name__}: {e}"
        raise
    finally:
        _log(run_id, region_key, status, error, timings, stats, round(time.time() - t_run, 1))
    return {"run_id": run_id, "timings": timings, "stats": stats}


def _recount(region_key: str) -> dict:
    from pyspark.sql import functions as F
    s = get()
    release = config.load("sources")["overture_buildings"]["release"]
    b = s.read.format("delta").load(config.table_path("bronze", "buildings")) \
        .where((F.col("_region") == region_key) & (F.col("_release") == release)).count()
    q = s.read.format("delta").load(config.table_path("silver", "buildings_quarantine")) \
        .where(F.col("_region") == region_key).count()
    chk = config.table_path("_tmp", f"silver_checked_{region_key}")
    out = s.read.format("delta").load(chk).where("NOT in_region").count()
    return {"bronze_rows": b, "quarantined": q, "outside_region": out}


def _log(run_id, region_key, status, error, timings, stats, seconds):
    try:
        row = dict(run_id=run_id, region=region_key, status=status, error=error,
                   seconds=float(seconds), timings=json.dumps(timings),
                   stats=json.dumps(stats, default=str), finished_at=datetime.now(UTC))
        # The published snapshot cannot contain the run that published it, so reports read this file.
        log = config.ROOT / "logs" / "runs.jsonl"
        log.parent.mkdir(exist_ok=True)
        with log.open("a") as f:
            f.write(json.dumps({**row, "data": str(config.DATA)}, default=str) + "\n")
        schema = ("run_id string, region string, status string, error string, seconds double, "
                  "timings string, stats string, finished_at timestamp")
        get().createDataFrame([row], schema).write.format("delta").mode("append") \
            .option("mergeSchema", "true").save(config.table_path("ops", "pipeline_runs"))
    except Exception as e:  # logging must never mask the real error
        print(f"(could not write run log: {e})")


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--region", default=None)
    r.add_argument("--from", dest="start", default="ingest", choices=STAGES)
    r.add_argument("--no-publish", action="store_true")
    r.add_argument("--full-refresh", action="store_true", help="rebuild gold from scratch")
    a = p.parse_args()
    try:
        out = run(config.region(a.region).key, a.start, a.no_publish, a.full_refresh)
        print(json.dumps(out, indent=2, default=str))
    finally:
        stop()


if __name__ == "__main__":
    main()
