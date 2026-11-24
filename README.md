# SolarAtlas India

**Rooftop solar potential for every building in Tamil Nadu: 25 million roofs, 38 districts, one lakehouse.**

SolarAtlas answers two questions: *how much power could Tamil Nadu's rooftops make?* and *for my
building, is solar worth the money?* It pulls every building footprint in the state from Overture Maps,
joins it with NASA satellite irradiance, places and land use, classifies each building
(home / shop / factory / institution), and prices a rooftop system for it under an approximate TNPDCL
tariff (unverified) and the PM Surya Ghar subsidy. It then rolls all of that up to wards, districts and
policy scenarios.

It's built as a production-style data platform: **PySpark + Delta Lake** medallion layers, a
**Spark ML** classifier, **Change Data Feed** incremental processing, a quality gate that blocks bad
data, atomic publishing to **DuckDB**, a **FastAPI + MapLibre** dashboard, and a **Databricks Asset
Bundle** that runs the same code as a 7-task job.

## Results: Tamil Nadu (measured 2026-11-24)

| | |
|---|---|
| Buildings analysed | **25,149,034** (30.6M downloaded, 5.45M outside the state clipped, 521 quarantined) |
| Full pipeline on a 16 GB laptop | **14.7 min** from empty tables, bronze → publish (4 GB driver) |
| Technical rooftop potential | 99.1 GW → 136 TWh/yr (upper bound, not a forecast) |
| Roofs that pay back within 7 years | **31.5 lakh** (9.6 GW), needing ₹51,423 crore of investment |
| Homes among them | 28.8 lakh |
| Building-type classifier | Spark ML random forest on 25k OSM labels; 81.1% held-out accuracy, 78.9% for the deployed rule |
| Tests | unit, API, Spark integration; 12-check quality gate on every run |

Full, regenerated numbers: [docs/RESULTS.md](docs/RESULTS.md) (written by `scripts/results.py`).

**What the data says**

- **Policy:** cheaper panels (−20% system cost) make more homes viable than raising the subsidy by a
  third (41.3 vs 38.8 lakh, from 28.8 lakh today) and cost **₹4,944 crore less** in subsidy.
  Withdrawing the subsidy cuts viable homes by 57%.
- **The 500-unit cliff:** homes on ≤500 units per bill get 200 free units, so solar is slow to pay back
  for light users (8.0 years at 350 units). Crossing 500 removes that help: median payback goes from
  5.2 years at 500 units to 3.9 at 501.
- **Metering rules matter:** for a 1,200-unit Chennai home with 3 kW, net metering saves 64% more per
  year than self-use only (₹38,806 vs ₹23,601; hourly simulation over all of 2025).
- **Where:** Coimbatore (7.4 GW), Tiruppur (6.1 GW, textile factory roofs) and Chennai (5.5 GW) lead.

## Architecture

```
Overture Maps (S3 GeoParquet)        NASA POWER API            Overture places / land use / divisions
  65 tiles, 3 parallel shards,         climatology + hourly       POIs, zones, state/district/ward
  DuckDB bbox pushdown, resumable      83 lattice points          polygons
            └─────────────────────────────┬───────────────────────────────┘
                     data/landing/   immutable files + manifests (rows, sha256)
                                          │
  ┌──────────────────────────── PySpark + Delta Lake ────────────────────────────────┐
  │ BRONZE   raw + lineage, partition replace (idempotent)                            │
  │ SILVER   geometry metrics (Arrow UDF) · broadcast STRtree join → district/ward/   │
  │          land use · clip to state · 7 quality rules → quarantine · MERGE on GERS  │
  │          id · Change Data Feed · hourly PV per kWp                                │
  │ FEATURES H3 density + POI rings (equality joins) · change-aware MERGE             │
  │ CLASSIFY Spark ML random forest → home / commercial / industrial / institutional  │
  │ GOLD     per-building economics by type · districts · wards · H3 hexes ·          │
  │          policy scenarios (aggregate-before-join) · incremental via CDF watermarks │
  │ QUALITY  12 checks → ops.dq_results; any critical failure = no publish            │
  └──────────────────────────────────────┬────────────────────────────────────────────┘
                     write → audit → publish (atomic symlink swap)
                     data/serving/snapshots/<run_id>/solaratlas.duckdb
                                          │
        FastAPI (read-only) → MapLibre dashboard: state hexes → city hexes → buildings,
        rankings, policy what-ifs, installer leads, hourly metering, printable reports, ops
```

## Quick start

Everything (Python, JDK, CLI tools, caches) lives inside this folder; nothing is installed system-wide.

```bash
./sa setup
```

```bash
./sa run --region tamil_nadu
```

```bash
./sa serve
```

Open http://localhost:8000. A city-sized region (`--region coimbatore`) runs in under a minute.

| Command | What it does |
|---|---|
| `./sa run --from silver` | resume from a stage (ingest, bronze, silver, features, classify, gold, quality, publish) |
| `./sa run --full-refresh` | rebuild gold from scratch instead of incrementally |
| `./sa test` / `./sa test-spark` | fast tests / Spark integration tests |
| `./sa maintain` | OPTIMIZE + Z-ORDER + VACUUM all Delta tables |
| `python scripts/results.py > docs/RESULTS.md` | regenerate measured results |
| `./sa clean` | delete caches and stray JVMs |

## Dashboard and API

Dashboard tabs: **Overview** (headline numbers, home calculator for the map's location, hourly
metering chart), **Rankings** (districts, wards), **Policy** (scenario comparison per district),
**Installers** (biggest viable roofs by type), **Pipeline** (runs, quality gate, classifier metrics).
Click any building for its numbers and a printable report.

| Endpoint | Returns |
|---|---|
| `/api/summary` | state headline numbers, potential by building type |
| `/api/districts`, `/api/localities?district=` | rankings |
| `/api/scenarios?district=` | policy what-ifs with change and subsidy per extra home |
| `/api/hexes?res=7\|8&bbox=` | H3 aggregates as GeoJSON |
| `/api/buildings?bbox=`, `/api/buildings/{id}` | footprints; one building with home or business economics |
| `/api/calculator?units=&lon=&lat=` | best system size, subsidy, payback for any home |
| `/api/metering?units=&kwp=&lon=&lat=` | hourly simulation: self-use, export, three metering rules |
| `/api/top-roofs?district=&building_type=` | installer leads |
| `/api/search?q=` · `/api/ops` · `/report/{id}` | search · pipeline health · printable report |

Interactive docs at `/docs`.

## Databricks

`databricks.yml` deploys a Unity Catalog schema, a managed volume and a job with tasks
bronze → silver → features → classify → gold → quality_gate → maintain, each running
`solaratlas/jobs/run_stage.py` (the same functions as the local runner).

```bash
databricks bundle deploy -t dev
```

Landing data is produced locally and uploaded with `scripts/upload_region.sh coimbatore` (one city) or
`scripts/upload_landing.sh` (everything). **Verified on Free Edition serverless (2026-11-24), all of Tamil Nadu:** 30,601,670 rows in,
25,149,034 buildings out, all 7 tasks and 12 checks passed, about 24 minutes of task time. Building
counts and technical potential equal the laptop build exactly; viable homes agree within 0.03%.

## Layout

```
config/      regions, sources, assumptions, tariffs, subsidy, non-domestic rates, scenarios, metering
solaratlas/  ingest/ (Overture tiles, divisions, places, land use, NASA POWER)
             lakehouse/ (spark, bronze, silver, spatial, features, classify, gold, quality, delta_utils)
             model/ (tariff engine, PV + economics, hourly metering) · serve/ · jobs/ · pipeline.py
api/         FastAPI app + report page        web/  dashboard (HTML/CSS/JS, no build step)
tests/       unit, API, Spark integration     docs/  DESIGN.md (every decision) · RESULTS.md
databricks.yml · .github/workflows/ci.yml
```

## Data, licences and honesty

- Buildings, divisions, land use: © OpenStreetMap contributors, Overture Maps Foundation, Google Open
  Buildings, Microsoft. **ODbL 1.0**; derived tables are ODbL too. Places: Overture places
  (CDLA-Permissive-2.0 / Apache-2.0 per source).
- Irradiance: NASA Langley Research Center POWER Project (2001–2025 climatology; hourly 2025).
- **Unverified inputs** (flagged in the UI): TN domestic slabs, commercial ₹8.80/unit, industrial
  ₹8.00/unit (assumption), feed-in ₹3/unit, ₹60k/kW home capex, household load shape, consumption
  estimated from footprint, crowding-based shading. Annual economics assume net metering per bill.

These are screening estimates from 2D footprints, not site surveys. See the limitations in
[docs/DESIGN.md](docs/DESIGN.md).
