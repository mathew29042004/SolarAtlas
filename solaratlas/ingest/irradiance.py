"""Fetch NASA POWER monthly climatology (irradiance + temperature) on a lattice over a region.

POWER's solar data come from a ~1 degree grid (meteorology ~0.5 degree), so sampling every
building would just repeat the same values. Instead we sample a 0.5 degree lattice covering the
region (only points near the state polygon for statewide regions) and each building later joins
to its nearest lattice point (its `cell_id`). Raw JSON responses are kept as-is
in the landing zone, one file per point, so the API is never re-hit for a point we already have.
"""
from __future__ import annotations

import argparse
import json
import math
import time
from datetime import UTC, datetime

import requests

from solaratlas import config

STEP = 0.5
PARAMS = "ALLSKY_SFC_SW_DWN,T2M"
EXPECTED_UNITS = {"ALLSKY_SFC_SW_DWN": "kW-hr/m^2/day", "T2M": "C"}


def cell_id(lon: float, lat: float, step: float = STEP) -> str:
    """Nearest lattice point, as a stable string key. Must match the Spark expression in silver."""
    return f"{round(lon / step) * step:.2f}_{round(lat / step) * step:.2f}"


def lattice(bbox, step: float = STEP):
    xmin, ymin, xmax, ymax = bbox
    xs = range(math.floor(xmin / step), math.ceil(xmax / step) + 1)
    ys = range(math.floor(ymin / step), math.ceil(ymax / step) + 1)
    return [(round(i * step, 2), round(j * step, 2)) for i in xs for j in ys]


def fetch(region_key: str | None = None, pause: float = 1.0) -> list[str]:
    src = config.load("sources")["nasa_power"]
    reg = config.region(region_key)
    out_dir = config.LANDING / "irradiance" / "nasa_power_climatology"
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []
    points = lattice(reg.bbox)
    reg_cfg = config.load("regions")["regions"][reg.key]
    if reg_cfg.get("state_code"):
        import shapely

        from solaratlas.ingest.statewide import state_polygon
        near = state_polygon(reg_cfg["state_code"]).buffer(STEP)
        points = [p for p in points if near.contains(shapely.Point(p))]
    print(f"[irradiance] {len(points)} lattice points for {reg.key}")
    for lon, lat in points:
        out = out_dir / f"cell={lon:.2f}_{lat:.2f}.json"
        if out.exists():
            continue
        for attempt in range(4):
            try:
                r = requests.get(
                    src["url"],
                    params={"parameters": PARAMS, "community": src["community"],
                            "longitude": lon, "latitude": lat, "format": "JSON"},
                    timeout=60,
                )
                r.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt == 3:
                    raise
                wait = 2 ** attempt * 5
                print(f"[irradiance] {lon},{lat}: {e}; retry in {wait}s")
                time.sleep(wait)
        body = r.json()
        for p, unit in EXPECTED_UNITS.items():
            got = body["parameters"][p]["units"]
            if got != unit:  # fail loudly: a silent unit change would corrupt every kWh number
                raise ValueError(f"NASA POWER changed units of {p}: {got!r} != {unit!r}")
        body["_fetched_at"] = datetime.now(UTC).isoformat()
        body["_request"] = {"lon": lon, "lat": lat}
        out.write_text(json.dumps(body))
        written.append(out.name)
        time.sleep(pause)  # be polite; POWER throttles bursts
    print(f"[irradiance] {len(written)} new point(s) for {reg.key}")
    return written


HOURLY_URL = "https://power.larc.nasa.gov/api/temporal/hourly/point"
HOURLY_UNITS = {"ALLSKY_SFC_SW_DWN": "Wh/m^2", "T2M": "C"}


def hourly_dir(year: int):
    return config.LANDING / "irradiance" / f"nasa_power_hourly_{year}"


def fetch_hourly(region_key: str | None = None, year: int = 2025, pause: float = 1.0) -> int:
    """One full year of hourly irradiance + temperature (local solar time) per lattice point.

    Used for the hourly self-consumption / net-metering simulation; same lattice as climatology.
    """
    clim = config.LANDING / "irradiance" / "nasa_power_climatology"
    points = [tuple(float(v) for v in f.stem.split("=")[1].split("_")) for f in clim.glob("cell=*.json")]
    out_dir = hourly_dir(year)
    out_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    for lon, lat in sorted(points):
        out = out_dir / f"cell={lon:.2f}_{lat:.2f}.json"
        if out.exists():
            continue
        for attempt in range(4):
            try:
                r = requests.get(HOURLY_URL, params={
                    "parameters": PARAMS, "community": "RE", "longitude": lon, "latitude": lat,
                    "start": f"{year}0101", "end": f"{year}1231", "format": "JSON",
                    "time-standard": "LST"}, timeout=120)
                r.raise_for_status()
                break
            except requests.RequestException as e:
                if attempt == 3:
                    raise
                time.sleep(2 ** attempt * 5)
                print(f"[hourly] {lon},{lat}: {e}; retrying")
        body = r.json()
        for p, unit in HOURLY_UNITS.items():
            if body["parameters"][p]["units"] != unit:
                raise ValueError(f"NASA POWER hourly units changed for {p}")
        keep = {"parameter": body["properties"]["parameter"], "_request": {"lon": lon, "lat": lat, "year": year},
                "_fetched_at": datetime.now(UTC).isoformat()}
        out.write_text(json.dumps(keep))
        n += 1
        time.sleep(pause)
    print(f"[hourly] {n} new point(s), year {year}")
    return n


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--region")
    p.add_argument("--hourly", type=int, help="fetch one year of hourly data, e.g. 2025")
    a = p.parse_args()
    if a.hourly:
        fetch_hourly(a.region, a.hourly)
    else:
        fetch(a.region)
