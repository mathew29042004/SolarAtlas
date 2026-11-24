"""SolarAtlas API: reads the published DuckDB snapshot (read-only) and serves the dashboard.

    uvicorn api.app:app --port 8000
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path

import duckdb
import h3
import shapely
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles

from api.report import render
from solaratlas import config
from solaratlas.ingest.irradiance import cell_id
from solaratlas.model import hourly, solar
from solaratlas.model import tariff as tariffs

DB = Path(os.environ.get("SA_SERVING_DB", config.SERVING / "current" / "solaratlas.duckdb"))
WEB = config.ROOT / "web"
MAX_FEATURES = 5000

app = FastAPI(title="SolarAtlas India API", version="0.2.0",
              description="Rooftop solar potential for every building in Tamil Nadu.")
_local = threading.local()


def db() -> duckdb.DuckDBPyConnection:
    target = DB.resolve()
    con = getattr(_local, "con", None)
    if con is None or getattr(_local, "path", None) != target:
        if not target.exists():
            raise HTTPException(503, "no published snapshot; run the pipeline first")
        con = duckdb.connect(str(target), read_only=True)
        _local.con, _local.path = con, target
    return con


def rows(sql: str, params=()) -> list[dict]:
    res = db().cursor().execute(sql, params)
    cols = [d[0] for d in res.description]
    return [dict(zip(cols, r, strict=True)) for r in res.fetchall()]


def has_table(name: str) -> bool:
    return bool(rows("SELECT 1 FROM information_schema.tables WHERE table_name = ?", [name]))


def default_region() -> str:
    built = [r["region"] for r in rows("SELECT region FROM region_summary ORDER BY buildings DESC")]
    want = config.region().key
    return want if want in built or not built else built[0]


@app.get("/health")
def health():
    return {"status": "ok", **rows("SELECT run_id, published_at FROM _meta")[0]}


@app.get("/api/summary")
def summary(region: str | None = None):
    region = region or default_region()
    r = rows("SELECT * EXCLUDE (_run_id) FROM region_summary WHERE region = ?", [region])
    if not r:
        raise HTTPException(404, f"region {region!r} not built")
    out = {k: v for k, v in r[0].items() if v is not None}
    out["by_type"] = rows(
        """SELECT building_type, count(*) AS buildings, round(sum(roof_kwp)/1000, 1) AS mwp,
                  round(sum(annual_kwh)/1e6, 1) AS gwh_year, sum(viable::int) AS viable,
                  round(median(payback_years), 1) AS median_payback
           FROM building_solar WHERE region = ? GROUP BY 1 ORDER BY mwp DESC""", [region])
    return out


@app.get("/api/districts")
def districts(region: str | None = None):
    return rows("SELECT * EXCLUDE (_run_id) FROM district_summary WHERE region = ? "
                "ORDER BY potential_gwh_year DESC", [region or default_region()])


@app.get("/api/localities")
def localities(district: str | None = None, region: str | None = None, limit: int = Query(50, le=2000)):
    region = region or default_region()
    if district:
        return rows("SELECT * EXCLUDE (_run_id) FROM locality_summary WHERE region = ? AND district = ? "
                    "ORDER BY potential_gwh_year DESC LIMIT ?", [region, district, limit])
    return rows("SELECT * EXCLUDE (_run_id) FROM locality_summary WHERE region = ? "
                "ORDER BY potential_gwh_year DESC LIMIT ?", [region, limit])


@app.get("/api/scenarios")
def scenarios(district: str | None = None, region: str | None = None):
    region = region or default_region()
    where, params = "region = ?", [region]
    if district:
        where += " AND district = ?"
        params.append(district)
    out = rows(f"""SELECT scenario, any_value(label) AS label, sum(homes) AS homes,
                          sum(viable_homes) AS viable_homes, sum(viable_nondomestic) AS viable_nondomestic,
                          round(sum(viable_mwp)::DOUBLE, 1) AS viable_mwp,
                          round(sum(subsidy_cr)::DOUBLE, 1) AS subsidy_cr,
                          round(sum(investment_cr)::DOUBLE, 1) AS investment_cr
                   FROM scenario_district WHERE {where} GROUP BY scenario""", params)
    base = next((r for r in out if r["scenario"] == "baseline"), None)
    for r in out:
        if base:
            extra = r["viable_homes"] - base["viable_homes"]
            r["delta_viable_homes"] = extra
            r["delta_subsidy_cr"] = round(r["subsidy_cr"] - base["subsidy_cr"], 1)
            r["subsidy_per_extra_home_inr"] = (
                round((r["subsidy_cr"] - base["subsidy_cr"]) * 1e7 / extra) if extra > 0 else None)
    order = list(config.load("scenarios")["scenarios"])
    return sorted(out, key=lambda r: order.index(r["scenario"]) if r["scenario"] in order else 99)


@app.get("/api/hexes")
def hexes(region: str | None = None, res: int = Query(7, ge=7, le=8), bbox: str | None = None):
    rs = rows("SELECT * EXCLUDE (_run_id) FROM hex_summary WHERE region = ? AND res = ?",
              [region or default_region(), res])
    box = shapely.box(*(float(v) for v in bbox.split(","))) if bbox else None
    feats = []
    for r in rs:
        lat, lng = h3.cell_to_latlng(r["h3"])
        if box is not None and not box.contains(shapely.Point(lng, lat)):
            continue
        ring = [[x, y] for y, x in h3.cell_to_boundary(r["h3"])]
        feats.append({"type": "Feature", "properties": r,
                      "geometry": {"type": "Polygon", "coordinates": [ring + ring[:1]]}})
    return {"type": "FeatureCollection", "features": feats}


@app.get("/api/buildings")
def buildings(bbox: str = Query(...), region: str | None = None,
              limit: int = Query(MAX_FEATURES, le=MAX_FEATURES)):
    try:
        x0, y0, x1, y1 = (float(v) for v in bbox.split(","))
    except ValueError as e:
        raise HTTPException(422, "bbox must be 4 comma-separated numbers") from e
    if (x1 - x0) * (y1 - y0) > 0.01:
        raise HTTPException(413, "bbox too large; zoom in or use /api/hexes")
    rs = rows("""SELECT id, geometry_wkb, footprint_m2, roof_kwp, annual_kwh, system_kwp, payback_years,
                        viable, building_type, type_source, size_class, source_dataset
                 FROM building_solar WHERE region = ? AND lon BETWEEN ? AND ? AND lat BETWEEN ? AND ?
                 ORDER BY annual_kwh DESC LIMIT ?""",
              [region or default_region(), x0, x1, y0, y1, limit])
    feats = [{"type": "Feature", "geometry": shapely.geometry.mapping(shapely.from_wkb(r.pop("geometry_wkb"))),
              "properties": r} for r in rs]
    return {"type": "FeatureCollection", "features": feats, "truncated": len(rs) == limit}


@app.get("/api/buildings/{building_id}")
def building(building_id: str, units: int | None = Query(None, ge=0, le=5000)):
    r = rows("SELECT * EXCLUDE (geometry_wkb, _run_id) FROM building_solar WHERE id = ? LIMIT 1", [building_id])
    if not r:
        raise HTTPException(404, "building not found")
    b = r[0]
    y = (b["annual_yield_kwh_kwp"] or 0) * (1 - (b["shading_loss"] or 0))
    b["household"] = b["business"] = None
    if b["building_type"] == "residential" and b["roof_kwp"]:
        b["household"] = calculator(units=units if units is not None else b["est_units"],
                                    kwp=None, max_kwp=b["roof_kwp"], cell_yield=y)
    elif b["roof_kwp"]:
        b["business"] = solar.nondomestic_economics(b["building_type"], b["roof_kwp"], y)
    return b


@app.get("/api/calculator")
def calculator(units: int = Query(..., ge=0, le=5000), kwp: float | None = Query(None, gt=0, le=10),
               max_kwp: float | None = Query(None, gt=0), lon: float | None = None,
               lat: float | None = None, region: str | None = None, cell_yield: float | None = None):
    if cell_yield is None:
        if lon is not None and lat is not None:
            y = rows("SELECT any_value(annual_yield_kwh_kwp) AS y FROM building_solar WHERE cell_id = ?",
                     [cell_id(lon, lat)])[0]["y"]
        else:
            y = rows("SELECT avg(annual_yield_kwh_kwp) AS y FROM building_solar WHERE region = ?",
                     [region or default_region()])[0]["y"]
        if y is None:
            raise HTTPException(404, "no climate data for that location")
        cell_yield = y
    tv = tariffs.as_of()
    if kwp:
        best = solar.household_economics(kwp, cell_yield, units, tv)
        options = [best]
    else:
        best, options = solar.best_size(cell_yield, units, tv, max_kwp)
    return {"units_bimonthly": units, "annual_yield_kwh_kwp": round(cell_yield, 1),
            "recommended": best, "options": options,
            "tariff": {"version": tv.version, "verified": tv.verified, "source": tv.source},
            "worth_it": best["lifetime_saving_inr"] > 0}


@app.get("/api/economics/curve")
def curve(region: str | None = None):
    return rows("""SELECT bimonthly_units, avg(best_kwp) AS best_kwp, avg(payback_years) AS payback_years,
                          avg(annual_saving_inr) AS annual_saving_inr, bool_and(worth_it) AS worth_it
                   FROM household_economics
                   WHERE cell_id IN (SELECT DISTINCT cell_id FROM building_solar WHERE region = ?)
                   GROUP BY 1 ORDER BY 1""", [region or default_region()])


_HOURLY: dict[tuple, tuple] = {}


def _cell_profile(cid: str):
    key = (str(DB.resolve()), cid)
    if key not in _HOURLY:
        if not has_table("cell_hourly"):
            raise HTTPException(503, "hourly profiles not published yet")
        r = db().cursor().execute("SELECT month, hour, pv_kwh_per_kwp FROM cell_hourly WHERE cell_id = ? "
                                  "ORDER BY ts", [cid]).fetchnumpy()
        if len(r["month"]) == 0:
            raise HTTPException(404, f"no hourly profile for cell {cid}")
        _HOURLY[key] = (r["month"].astype(int), r["hour"].astype(int), r["pv_kwh_per_kwp"].astype(float))
    return _HOURLY[key]


@app.get("/api/metering")
def metering(units: int = Query(..., ge=50, le=5000), kwp: float | None = Query(None, gt=0, le=10),
             lon: float = 76.96, lat: float = 11.0):
    """Hourly simulation for one home: self-use, export and the bill under three metering rules."""
    months, hours, pv = _cell_profile(cell_id(lon, lat))
    if kwp is None:
        annual = float(pv.sum())
        kwp = solar.best_size(annual, units, tariffs.as_of())[0]["kwp"]
    out = hourly.simulate(months, hours, pv, kwp, units)
    out["cell_id"] = cell_id(lon, lat)
    out["assumptions"] = "Load shape and feed-in rate are assumptions (config/metering.yaml)."
    return out


@app.get("/api/top-roofs")
def top_roofs(district: str | None = None, building_type: str | None = None,
              region: str | None = None, limit: int = Query(100, le=1000)):
    where, params = ["region = ?", "viable"], [region or default_region()]
    if district:
        where.append("district = ?")
        params.append(district)
    if building_type:
        where.append("building_type = ?")
        params.append(building_type)
    return rows(f"""SELECT id, district, locality, building_type, round(lon, 6) AS lon, round(lat, 6) AS lat,
                           footprint_m2, system_kwp, annual_kwh, capex_inr, annual_saving_inr, payback_years
                    FROM building_solar WHERE {' AND '.join(where)}
                    ORDER BY system_kwp DESC, payback_years LIMIT ?""", [*params, limit])


@app.get("/api/search")
def search(q: str = Query(..., min_length=2), region: str | None = None):
    region, like = region or default_region(), f"%{q}%"
    out = [{"kind": "district", **r} for r in rows(
        "SELECT district AS name, district, lon, lat, potential_mwp FROM district_summary "
        "WHERE region = ? AND district ILIKE ? LIMIT 5", [region, like])]
    out += [{"kind": "locality", **r} for r in rows(
        "SELECT locality AS name, district, lon, lat, potential_mwp FROM locality_summary "
        "WHERE region = ? AND locality ILIKE ? ORDER BY potential_mwp DESC LIMIT 10", [region, like])]
    return out


@app.get("/api/tariffs")
def tariff_versions():
    return rows("SELECT * EXCLUDE (_run_id) FROM dim_tariff ORDER BY effective_from, band, from_units")


@app.get("/api/ops")
def ops():
    out = {"snapshot": health()}
    if has_table("pipeline_runs"):
        out["runs"] = rows("SELECT run_id, region, status, error, seconds, timings, finished_at "
                           "FROM pipeline_runs ORDER BY finished_at DESC LIMIT 20")
    if has_table("dq_results"):
        out["quality"] = rows('SELECT "check", severity, passed, observed, expected FROM dq_results '
                              "WHERE run_id = (SELECT max(run_id) FROM dq_results)")
    if has_table("model_metrics"):
        m = rows("SELECT metrics FROM model_metrics ORDER BY trained_at DESC LIMIT 1")
        out["classifier"] = json.loads(m[0]["metrics"]) if m else None
    return out


@app.get("/report/{building_id}", response_class=HTMLResponse)
def report(building_id: str, units: int | None = Query(None, ge=0, le=5000)):
    return render(building(building_id, units))


@app.get("/")
def index():
    return FileResponse(WEB / "index.html")


app.mount("/static", StaticFiles(directory=WEB), name="static")
