"""Resumable statewide building extract, tile by tile.

The state bbox is cut into non-overlapping STEP-degree tiles. A building belongs to the tile
holding its bbox's south-west corner, so every building lands in exactly one tile with no
duplicates. Tiles that do not touch the state polygon are skipped. Each tile is an atomic landing
file with its own manifest, so a dropped connection only loses the tile in flight, and rerunning
continues where it stopped.

One DuckDB connection with the HTTP/Parquet metadata caches on reads the ~500 planet file
footers once, not once per tile.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
from datetime import UTC, datetime

import duckdb
import shapely

from solaratlas import config
from solaratlas.ingest import divisions

STEP = 0.5


def state_polygon(state_code: str):
    release = config.load("sources")["overture_buildings"]["release"]
    f = divisions.landing_dir(release, state_code) / "division_area.parquet"
    if not f.exists():
        divisions.extract(state_code)
    wkb = duckdb.connect().execute(
        f"SELECT geometry_wkb FROM '{f}' WHERE level = 'region' LIMIT 1").fetchone()[0]
    return shapely.from_wkb(wkb)


def tiles(state_code: str, step: float = STEP) -> list[tuple[float, float]]:
    poly = state_polygon(state_code)
    xmin, ymin, xmax, ymax = poly.bounds
    out = []
    for i in range(math.floor(xmin / step), math.ceil(xmax / step)):
        for j in range(math.floor(ymin / step), math.ceil(ymax / step)):
            x, y = round(i * step, 4), round(j * step, 4)
            # a building whose SW corner is in this tile can poke out by its own size; the
            # small buffer keeps edge tiles whose buildings still overlap the state
            if shapely.box(x, y, x + step, y + step).buffer(0.01).intersects(poly):
                out.append((x, y))
    return out


def tile_dir(release: str, region_key: str, x: float, y: float):
    return (config.LANDING / "buildings" / f"release={release}" / f"region={region_key}"
            / f"tile={x:.2f}_{y:.2f}")


def extract(region_key: str = "tamil_nadu", limit: int | None = None,
            shard: tuple[int, int] = (0, 1)) -> dict:
    reg_cfg = config.load("regions")["regions"][region_key]
    state = reg_cfg["state_code"]
    src = config.load("sources")["overture_buildings"]
    release = src["release"]
    url = src["url"].format(release=release)
    todo = tiles(state)
    k, n_shards = shard   # parallel workers take disjoint tiles: the network throttles per connection
    mine = {t for i, t in enumerate(todo) if i % n_shards == k}
    con = divisions.connect()
    con.execute("SET enable_http_metadata_cache = true; SET parquet_metadata_cache = true;")
    con.execute("SET http_retries = 8; SET http_timeout = 120000;")
    done = rows_total = bytes_total = 0
    t_all = time.time()
    for n, (x, y) in enumerate(todo, 1):
        d = tile_dir(release, region_key, x, y)
        man = d / "_manifest.json"
        if (x, y) not in mine and not man.exists():
            continue
        if man.exists():
            m = json.loads(man.read_text())
            rows_total += m["rows"]
            bytes_total += m["bytes"]
            done += 1
            continue
        if limit is not None and n > limit:
            break
        d.mkdir(parents=True, exist_ok=True)
        out, tmp = d / "buildings.parquet", d / "buildings.parquet.tmp"
        t0 = time.time()
        for attempt in range(5):
            try:
                con.execute(f"""
                    COPY (
                      SELECT id, geometry AS geometry_wkb,
                             bbox.xmin AS xmin, bbox.ymin AS ymin, bbox.xmax AS xmax, bbox.ymax AS ymax,
                             height, num_floors, roof_shape, roof_material, class, subtype,
                             sources[1].dataset AS source_dataset,
                             sources[1].confidence AS source_confidence
                      FROM read_parquet('{url}', hive_partitioning = false)
                      WHERE bbox.xmin >= {x} AND bbox.xmin < {x + STEP}
                        AND bbox.ymin >= {y} AND bbox.ymin < {y + STEP}
                    ) TO '{tmp}' (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 250000)""")
                break
            except duckdb.Error as e:
                if attempt == 4:
                    raise
                wait = 15 * 2 ** attempt
                print(f"  tile {x},{y} failed ({e.__class__.__name__}: {str(e)[:120]}); retry in {wait}s",
                      flush=True)
                time.sleep(wait)
        tmp.rename(out)
        rows = duckdb.connect().execute(f"SELECT count(*) FROM '{out}'").fetchone()[0]
        meta = {"tile": [x, y, STEP], "rows": rows, "bytes": out.stat().st_size,
                "sha256": hashlib.sha256(out.read_bytes()).hexdigest(), "release": release,
                "seconds": round(time.time() - t0, 1), "extracted_at": datetime.now(UTC).isoformat()}
        man.write_text(json.dumps(meta))
        rows_total += rows
        bytes_total += meta["bytes"]
        done += 1
        print(f"[{n}/{len(todo)}] tile {x:.2f},{y:.2f}: {rows:,} rows {meta['bytes']/1e6:.1f} MB "
              f"{meta['seconds']}s | total {rows_total:,} rows {bytes_total/1e9:.2f} GB", flush=True)
    summary = {"region": region_key, "tiles_total": len(todo), "tiles_done": done,
               "rows": rows_total, "bytes": bytes_total, "seconds": round(time.time() - t_all, 1)}
    if n_shards == 1:
        (config.LANDING / "buildings" / f"release={release}" / f"region={region_key}"
         / "_summary.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary))
    return summary


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--region", default="tamil_nadu")
    p.add_argument("--limit", type=int)
    p.add_argument("--list", action="store_true")
    p.add_argument("--shard", default="0/1", help="k/n: this worker takes tiles where i %% n == k")
    a = p.parse_args()
    if a.list:
        print(len(tiles(config.load("regions")["regions"][a.region]["state_code"])))
    else:
        k, n = (int(v) for v in a.shard.split("/"))
        extract(a.region, a.limit, (k, n))
