"""Data quality gate. Every check is recorded in ops.dq_results; any `critical` failure stops the run
before anything is published to serving (write-audit-publish)."""
from __future__ import annotations

from datetime import UTC, datetime

from pyspark.sql import functions as F

from solaratlas import config
from solaratlas.lakehouse.spark import get


class QualityGateFailed(RuntimeError):
    pass


def _t(layer, name):
    return get().read.format("delta").load(config.table_path(layer, name))


def run(region_key: str, run_id: str, stats: dict) -> list[dict]:
    silver = _t("silver", "buildings").where(F.col("region") == region_key)
    gold = _t("gold", "building_solar").where(F.col("region") == region_key)
    n_bronze = stats["bronze_rows"]
    n_q = stats["quarantined"]
    n_out = stats.get("outside_region", 0)
    n_silver = silver.count()
    dupes = n_silver - silver.select("id").distinct().count()
    no_cell = gold.where(F.col("annual_yield_kwh_kwp").isNull()).count()
    y = gold.agg(F.min("annual_yield_kwh_kwp"), F.max("annual_yield_kwh_kwp")).first()
    months = _t("silver", "cell_climate").groupBy("cell_id").count().where("count != 12").count()
    checks = [
        ("silver_not_empty", "critical", n_silver > 0, n_silver, "> 0"),
        ("bronze_reconciles", "critical", n_bronze == n_silver + n_q + n_out,
         f"{n_bronze} = {n_silver} + {n_q} + {n_out}", "bronze = silver + quarantine + outside"),
        ("quarantine_rate", "critical", n_q / max(n_bronze - n_out, 1) < 0.05,
         round(n_q / max(n_bronze - n_out, 1), 4), "< 0.05"),
        ("silver_unique_id", "critical", dupes == 0, dupes, "= 0"),
        ("every_building_has_climate", "critical", no_cell == 0, no_cell, "= 0"),
        ("climate_has_12_months", "critical", months == 0, months, "= 0 cells missing months"),
        ("yield_plausible_india", "critical",
         y[0] is not None and 1100 <= y[0] and y[1] <= 1900, f"{y[0]:.0f}-{y[1]:.0f}" if y[0] else None,
         "1100-1900 kWh/kWp/yr"),
        ("gold_matches_silver", "critical", gold.count() == n_silver, gold.count(), f"= {n_silver}"),
        ("every_building_has_type", "critical", gold.where(F.col("building_type").isNull()).count() == 0,
         gold.where(F.col("building_type").isNull()).count(), "= 0"),
        ("district_coverage", "critical",
         gold.where(F.col("district").isNull()).count() / max(n_silver, 1) < 0.01,
         round(gold.where(F.col("district").isNull()).count() / max(n_silver, 1), 4), "< 1% without district"),
        ("shading_in_range", "critical",
         gold.where((F.col("shading_loss") < 0) | (F.col("shading_loss") > 0.15)).count() == 0,
         gold.agg(F.max("shading_loss")).first()[0], "0..0.15"),
        ("payback_sane", "warning",
         gold.where((F.col("payback_years") < 0.5) & F.col("viable")).count() == 0,
         gold.where((F.col("payback_years") < 0.5) & F.col("viable")).count(), "no viable payback < 0.5 yr"),
    ]
    now = datetime.now(UTC)
    rows = [dict(run_id=run_id, region=region_key, check=c, severity=s, passed=bool(p),
                 observed=str(o), expected=e, checked_at=now) for c, s, p, o, e in checks]
    get().createDataFrame(rows).write.format("delta").mode("append") \
        .save(config.table_path("ops", "dq_results"))
    failed = [r for r in rows if r["severity"] == "critical" and not r["passed"]]
    for r in rows:
        print(f"  [{'PASS' if r['passed'] else 'FAIL'}] {r['check']}: {r['observed']} ({r['expected']})")
    if failed:
        raise QualityGateFailed(", ".join(r["check"] for r in failed))
    return rows
