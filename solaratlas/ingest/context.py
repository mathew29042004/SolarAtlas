"""Context layers for classifying buildings: Overture places (POIs) and land-use polygons.

places   : point, category, confidence, licence (mixed: CDLA-Permissive-2.0 / Apache-2.0 per row)
land_use : polygon, subtype/class (OSM-derived, ODbL)
One file per (release, state), clipped to the state bbox; kept raw for the lakehouse.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime

import duckdb

from solaratlas import config
from solaratlas.ingest import divisions

QUERIES = {
    "places": """
        SELECT id, bbox.xmin AS lon, bbox.ymin AS lat, basic_category,   -- point: bbox = position
               taxonomy.primary AS category, taxonomy.hierarchy AS hierarchy, confidence,
               names.primary AS name, addresses[1].freeform AS address,
               sources[1].dataset AS source_dataset, sources[1].license AS licence
        FROM read_parquet('{base}/theme=places/type=place/*.parquet', hive_partitioning = false)
        WHERE bbox.xmin >= {x0} AND bbox.xmax <= {x1} AND bbox.ymin >= {y0} AND bbox.ymax <= {y1}
          AND confidence >= 0.4""",
    "land_use": """
        SELECT id, subtype, class, names.primary AS name, geometry AS geometry_wkb,
               bbox.xmin AS xmin, bbox.ymin AS ymin, bbox.xmax AS xmax, bbox.ymax AS ymax
        FROM read_parquet('{base}/theme=base/type=land_use/*.parquet', hive_partitioning = false)
        WHERE bbox.xmin >= {x0} AND bbox.xmax <= {x1} AND bbox.ymin >= {y0} AND bbox.ymax <= {y1}
          AND subtype IN ('residential', 'developed', 'education', 'medical', 'religious',
                          'military', 'transportation', 'agriculture', 'campground', 'park')""",
}


def landing_dir(layer: str, release: str, state_code: str):
    return config.LANDING / layer / f"release={release}" / f"state={state_code}"


def extract(layer: str, region_key: str = "tamil_nadu", force: bool = False) -> dict:
    reg = config.load("regions")["regions"][region_key]
    release = config.load("sources")["overture_buildings"]["release"]
    out_dir = landing_dir(layer, release, reg["state_code"])
    out, man = out_dir / f"{layer}.parquet", out_dir / "_manifest.json"
    if man.exists() and not force:
        print(f"[{layer}] already landed")
        return json.loads(man.read_text())
    out_dir.mkdir(parents=True, exist_ok=True)
    x0, y0, x1, y1 = reg["bbox"]
    con = divisions.connect()
    con.execute("SET enable_http_metadata_cache = true; SET http_retries = 8;")
    tmp = out.with_suffix(".tmp")
    sql = QUERIES[layer].format(base=f"s3://overturemaps-us-west-2/release/{release}",
                                x0=x0, x1=x1, y0=y0, y1=y1)
    con.execute(f"COPY ({sql}) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)")
    tmp.rename(out)
    rows = duckdb.connect().execute(f"SELECT count(*) FROM '{out}'").fetchone()[0]
    meta = {"layer": layer, "release": release, "state": reg["state_code"], "rows": rows,
            "bytes": out.stat().st_size, "sha256": hashlib.sha256(out.read_bytes()).hexdigest(),
            "extracted_at": datetime.now(UTC).isoformat()}
    man.write_text(json.dumps(meta, indent=2))
    print(f"[{layer}] {rows:,} rows {meta['bytes']/1e6:.1f} MB")
    return meta


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("layer", choices=list(QUERIES))
    p.add_argument("--region", default="tamil_nadu")
    p.add_argument("--force", action="store_true")
    a = p.parse_args()
    extract(a.layer, a.region, a.force)
