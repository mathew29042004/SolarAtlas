"""Run one pipeline stage: the entry point for each task of the Databricks Job.

    python -m solaratlas.jobs.run_stage --stage silver --region tamil_nadu --run-id <job run id>

Stages are the same functions the local runner calls; state passes between tasks through Delta
tables only (no in-memory handoff), so any task can be retried on its own.
"""
from __future__ import annotations

import argparse
import json
import os
import sys

# make the bundle's source folder importable when Databricks runs this file directly
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from solaratlas import config  # noqa: E402
from solaratlas.lakehouse import bronze, classify, features, gold, quality, silver  # noqa: E402
from solaratlas.lakehouse.spark import stop  # noqa: E402


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--stage", required=True,
                   choices=["bronze", "silver", "features", "classify", "gold", "quality", "maintain"])
    p.add_argument("--region", default="tamil_nadu")
    p.add_argument("--run-id", default="manual")
    p.add_argument("--data", help="override SA_DATA (e.g. a Unity Catalog volume path)")
    a = p.parse_args(argv)
    if a.data:
        os.environ["SA_DATA"] = a.data
    region, run_id = config.region(a.region).key, a.run_id
    out: dict = {}
    if a.stage == "bronze":
        out["buildings"] = bronze.buildings(region, run_id)
        out["irradiance"] = bronze.irradiance(run_id)
        out["irradiance_hourly"] = bronze.irradiance_hourly(run_id)
    elif a.stage == "silver":
        out.update(silver.buildings(region, run_id))
        out["places"] = silver.places(region, run_id)
        out["climate"] = silver.cell_climate(run_id)
        out["hourly"] = silver.cell_hourly(run_id)
    elif a.stage == "features":
        out["features"] = features.build(region, run_id)
    elif a.stage == "classify":
        m = classify.train_and_predict(region, run_id)
        out = {k: m.get(k) for k in ("accuracy", "macro_f1", "labels")}
    elif a.stage == "gold":
        out["building_solar"] = gold.building_solar(region, run_id)
        out.update(gold.area_summaries(region, run_id))
        out["scenarios"] = gold.scenarios(region, run_id)
        out["household_economics"] = gold.household_economics(run_id)
        out["tariffs"] = gold.dim_tariff(run_id)
        gold.region_summary(region, run_id)
    elif a.stage == "quality":
        from solaratlas.pipeline import _recount
        quality.run(region, run_id, _recount(region))
    elif a.stage == "maintain":
        from solaratlas import maintenance
        maintenance.main()
    print(json.dumps({"stage": a.stage, "region": region, **out}, default=str))
    stop()


if __name__ == "__main__":
    main()
