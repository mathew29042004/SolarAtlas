"""Administrative areas for a state from Overture's divisions theme (ODbL).

Levels kept: region (state) -> county (district) -> locality (town/village) -> neighborhood.
Written once per (release, state) to the landing zone, like buildings.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime

import duckdb

from solaratlas import config

LEVELS = ("region", "county", "locality", "neighborhood", "macrohood", "microhood")


def landing_dir(release: str, state_code: str):
    return config.LANDING / "divisions" / f"release={release}" / f"state={state_code}"


def latest_file(state_code: str):
    """Newest landed division_area file for a state. Boundaries change far less often than
    buildings, so a new building release reuses the latest boundaries already on disk."""
    files = sorted((config.LANDING / "divisions").glob(f"release=*/state={state_code}/division_area.parquet"))
    if not files:
        raise FileNotFoundError(f"no divisions landed for {state_code}; run ingest.divisions first")
    return files[-1]


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET home_directory='{config.ROOT / '.cache'}'")
    con.execute(f"SET extension_directory='{config.ROOT / '.cache' / 'duckdb_ext'}'")
    con.execute("INSTALL httpfs; LOAD httpfs; SET s3_region='us-west-2'; SET enable_progress_bar=false")
    return con


def extract(state_code: str = "IN-TN", force: bool = False) -> dict:
    release = config.load("sources")["overture_buildings"]["release"]
    out_dir = landing_dir(release, state_code)
    out, manifest = out_dir / "division_area.parquet", out_dir / "_manifest.json"
    if out.exists() and manifest.exists() and not force:
        print(f"[divisions] already landed: {out}")
        return json.loads(manifest.read_text())
    out_dir.mkdir(parents=True, exist_ok=True)
    url = (f"s3://overturemaps-us-west-2/release/{release}/theme=divisions/"
           "type=division_area/*.parquet")
    tmp = out.with_suffix(".tmp")
    levels = ", ".join(f"'{lvl}'" for lvl in LEVELS)
    connect().execute(f"""
        COPY (
          SELECT id, division_id, subtype AS level, names.primary AS name, region AS state_code,
                 geometry AS geometry_wkb, bbox.xmin AS xmin, bbox.ymin AS ymin,
                 bbox.xmax AS xmax, bbox.ymax AS ymax, is_land
          FROM read_parquet('{url}')
          WHERE region = '{state_code}' AND subtype IN ({levels})
        ) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)""")
    tmp.rename(out)
    con = duckdb.connect()
    counts = dict(con.execute(f"SELECT level, count(*) FROM '{out}' GROUP BY 1").fetchall())
    bbox = con.execute(f"SELECT min(xmin), min(ymin), max(xmax), max(ymax) FROM '{out}' "
                       "WHERE level = 'region'").fetchone()
    meta = {"source": "overture_divisions", "release": release, "state": state_code,
            "levels": counts, "state_bbox": [round(v, 4) for v in bbox],
            "sha256": hashlib.sha256(out.read_bytes()).hexdigest(),
            "extracted_at": datetime.now(UTC).isoformat(), "licence": "ODbL-1.0"}
    manifest.write_text(json.dumps(meta, indent=2))
    print(f"[divisions] {counts} bbox={meta['state_bbox']}")
    return meta


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--state", default="IN-TN")
    p.add_argument("--force", action="store_true")
    a = p.parse_args()
    extract(a.state, a.force)
