"""Point-in-polygon joins for tens of millions of buildings without a geospatial engine.

Polygons (districts, localities, land use) are small enough to broadcast. Each Python worker
builds a shapely STRtree once (cached per process) and answers a whole Arrow batch of points
in one vectorised `tree.query(points, predicate="within")` call. That's ~1M points/s per core,
and the same code runs on Databricks.
"""
from __future__ import annotations

import duckdb
import numpy as np
import pandas as pd
import shapely
from pyspark.sql import types as T
from pyspark.sql.functions import pandas_udf

from solaratlas import config
from solaratlas.ingest import divisions

SIMPLIFY_DEG = 0.0002   # ~20 m: shrinks broadcast size; only buildings within ~20 m of a boundary can move

_TREES: dict[str, tuple] = {}


def admin_polygons(state_code: str, level: str) -> list[tuple[str, str, bytes]]:
    """(division_id, name, simplified WKB) for one admin level of a state."""
    f = divisions.latest_file(state_code)
    rows = duckdb.connect().execute(
        f"SELECT division_id, name, geometry_wkb FROM '{f}' WHERE level = ? ORDER BY name", [level]
    ).fetchall()
    out = []
    for did, name, wkb in rows:
        g = shapely.make_valid(shapely.from_wkb(wkb)).simplify(SIMPLIFY_DEG, preserve_topology=True)
        out.append((did, name, shapely.to_wkb(g)))
    return out


def landuse_polygons(state_code: str) -> list[tuple[str, str, bytes]]:
    release = config.load("sources")["overture_buildings"]["release"]
    f = config.LANDING / "land_use" / f"release={release}" / f"state={state_code}" / "land_use.parquet"
    if not f.exists():
        return []
    rows = duckdb.connect().execute(
        f"SELECT id, subtype || ':' || coalesce(class, ''), geometry_wkb FROM '{f}'").fetchall()
    out = []
    for i, label, wkb in rows:
        g = shapely.from_wkb(wkb)
        if g is not None and g.geom_type in ("Polygon", "MultiPolygon"):
            out.append((i, label, shapely.to_wkb(g.simplify(0.00005, preserve_topology=True))))
    return out


def _tree(key: str, polys: list[tuple[str, str, bytes]]):
    if key not in _TREES:
        geoms = shapely.from_wkb([p[2] for p in polys])
        names = np.array([p[1] for p in polys], dtype=object)
        ids = np.array([p[0] for p in polys], dtype=object)
        areas = shapely.area(geoms)
        _TREES[key] = (shapely.STRtree(geoms), names, ids, areas)
    return _TREES[key]


def lookup(key: str, polys, lon: np.ndarray, lat: np.ndarray, smallest: bool = False):
    """For each point, the name and id of the containing polygon (or None).

    smallest=True picks the smallest containing polygon (nested land use: a school inside a
    residential zone counts as the school).
    """
    tree, names, ids, areas = _tree(key, polys)
    pts = shapely.points(lon, lat)
    out_name = np.full(len(pts), None, dtype=object)
    out_id = np.full(len(pts), None, dtype=object)
    if len(polys) == 0 or len(pts) == 0:
        return out_name, out_id
    pi, gi = tree.query(pts, predicate="within")
    if smallest and len(pi):
        order = np.lexsort((areas[gi], pi))     # per point, smallest area first
        pi, gi = pi[order], gi[order]
    first = np.unique(pi, return_index=True)[1]  # keep one polygon per point
    out_name[pi[first]] = names[gi[first]]
    out_id[pi[first]] = ids[gi[first]]
    return out_name, out_id


ADMIN_SCHEMA = T.StructType([
    T.StructField("district", T.StringType()),
    T.StructField("district_id", T.StringType()),
    T.StructField("locality", T.StringType()),
    T.StructField("locality_id", T.StringType()),
    T.StructField("in_state", T.BooleanType()),
    T.StructField("landuse", T.StringType()),
])


def admin_udf(state_code: str):
    """pandas UDF (lon, lat) -> admin areas + land use. Polygons ride along in the closure."""
    state = admin_polygons(state_code, "region")
    districts = admin_polygons(state_code, "county")
    localities = admin_polygons(state_code, "locality")
    landuse = landuse_polygons(state_code)

    @pandas_udf(ADMIN_SCHEMA)
    def assign(lon: pd.Series, lat: pd.Series) -> pd.DataFrame:
        x, y = lon.to_numpy(), lat.to_numpy()
        in_state, _ = lookup(f"{state_code}:region", state, x, y)
        d, did = lookup(f"{state_code}:county", districts, x, y)
        # nested localities (a metro area containing wards): the most specific one wins
        loc, lid = lookup(f"{state_code}:locality", localities, x, y, smallest=True)
        lu, _ = lookup(f"{state_code}:landuse", landuse, x, y, smallest=True)
        return pd.DataFrame({"district": d, "district_id": did, "locality": loc, "locality_id": lid,
                             "in_state": pd.notna(in_state), "landuse": lu})

    return assign
