"""Extract building footprints for one region from Overture Maps into the landing zone.

Overture publishes the planet as ~500 spatially sorted GeoParquet files on public S3. DuckDB
pushes the bbox filter down to Parquet row-group statistics, so only the row groups that
overlap the region are downloaded: tens of MB instead of hundreds of GB.

The landing file is written once per (release, region) and never modified: re-running is a
no-op unless --force is given. That makes the extract idempotent and replayable.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import UTC, datetime

import duckdb

from solaratlas import config


def landing_dir(release: str, region_key: str):
    return config.LANDING / "buildings" / f"release={release}" / f"region={region_key}"


def extract(region_key: str | None = None, force: bool = False) -> dict:
    src = config.load("sources")["overture_buildings"]
    release = src["release"]
    reg = config.region(region_key)
    out_dir = landing_dir(release, reg.key)
    out = out_dir / "buildings.parquet"
    manifest = out_dir / "_manifest.json"
    if out.exists() and manifest.exists() and not force:
        print(f"[buildings] already landed: {out} (use --force to re-extract)")
        return json.loads(manifest.read_text())

    out_dir.mkdir(parents=True, exist_ok=True)
    xmin, ymin, xmax, ymax = reg.bbox
    url = src["url"].format(release=release)
    con = duckdb.connect()
    con.execute(f"SET home_directory='{config.ROOT / '.cache'}'")
    con.execute(f"SET extension_directory='{config.ROOT / '.cache' / 'duckdb_ext'}'")
    con.execute("INSTALL httpfs; LOAD httpfs; SET s3_region='us-west-2';")
    con.execute("SET enable_progress_bar=false")
    t0 = time.time()
    tmp = out.with_suffix(".parquet.tmp")
    # Keep only what the model needs; geometry stays WKB so no spatial extension is required.
    con.execute(
        f"""
        COPY (
          SELECT id,
                 geometry                                  AS geometry_wkb,
                 bbox.xmin AS xmin, bbox.ymin AS ymin, bbox.xmax AS xmax, bbox.ymax AS ymax,
                 height, num_floors, roof_shape, roof_material, class, subtype,
                 sources[1].dataset                        AS source_dataset,
                 sources[1].confidence                     AS source_confidence
          FROM read_parquet('{url}', hive_partitioning = false)
          WHERE bbox.xmin >= {xmin} AND bbox.xmax <= {xmax}
            AND bbox.ymin >= {ymin} AND bbox.ymax <= {ymax}
        ) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd)
        """
    )
    tmp.rename(out)  # atomic: a crash never leaves a half-written landing file
    rows = con.execute(f"SELECT count(*) FROM '{out}'").fetchone()[0]
    meta = {
        "source": "overture_buildings",
        "release": release,
        "region": reg.key,
        "bbox": list(reg.bbox),
        "rows": rows,
        "bytes": out.stat().st_size,
        "sha256": hashlib.sha256(out.read_bytes()).hexdigest(),
        "extracted_at": datetime.now(UTC).isoformat(),
        "seconds": round(time.time() - t0, 1),
        "licence": src["licence"],
    }
    manifest.write_text(json.dumps(meta, indent=2))
    print(f"[buildings] {rows:,} rows, {meta['bytes']/1e6:.1f} MB in {meta['seconds']} s -> {out}")
    return meta


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--region")
    p.add_argument("--force", action="store_true")
    a = p.parse_args()
    extract(a.region, a.force)
