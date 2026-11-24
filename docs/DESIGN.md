# Design decisions

Each decision with its reason and the alternative it beat, so it can be defended in an interview.

## Data sources

**Overture Maps buildings instead of raw Google Open Buildings.** Overture merges OSM, Google
and Microsoft footprints, de-duplicates them under a stable global id (GERS), and publishes
spatially sorted GeoParquet on public S3. In Coimbatore it gave 40% OSM, 47% Google and 13% Microsoft
footprints: better than any one source alone. Google's own release is gzipped CSV by S2 cell
(GBs per tile for India). Licence: ODbL, so derived tables are shared alike.

**DuckDB for extraction, not Spark.** Reading one city out of ~500 × 500 MB files is a
filter-pushdown problem: DuckDB reads Parquet footers, skips row groups whose bbox statistics
miss the city, and downloads only 31 MB. Spark could do the same, but it would need S3 jars and
a JVM just to filter. The right tool for each stage.

**NASA POWER climatology on a 0.25° lattice.** POWER's solar parameter comes from a ~1° grid, so
querying per building would repeat identical numbers 343k times and hammer a free API. One request
per lattice point (9 for Coimbatore), stored raw, never re-fetched. The ingest **checks units
in every response** (`kW-hr/m^2/day`): a silent unit change would corrupt every kWh downstream.

## Lakehouse

**Landing zone is immutable.** Files are written to `*.tmp` then renamed (atomic), with a manifest
holding row count, sha256, release and licence. Any table can be rebuilt from landing alone.

**Bronze uses `replaceWhere` on (region, release).** A rerun replaces exactly its own partition:
no duplicates, other regions untouched. Measured: a rerun gives identical row counts in every layer.

**Silver quarantines instead of dropping.** Seven rules (null id, invalid geometry, non-polygon,
area < 2 m², area > 50,000 m², centroid outside region, height ≤ 0 or > 300 m, duplicate id).
Failing rows go to `silver.buildings_quarantine` with every reason as an array, so you can see
*why* data was rejected. Duplicates keep the highest-confidence copy.

**Silver MERGE on the GERS id, with `whenNotMatchedBySource DELETE` scoped to the region.**
When Overture ships a new release, changed buildings update, new ones insert and demolished ones
are deleted, but only within that region. Change Data Feed is on, so downstream consumers can
read only what changed. Covered by `test_new_release_merges_updates_and_deletes`.

**Footprint area without a geospatial engine.** Areas use a local equirectangular projection:
scaling x by cos(lat) is linear, so m² = deg² × cos(lat) × 111,320 × 110,574. For buildings
(tens of metres) the error is far below 0.1% (a test checks a 10 m square comes out at 100 m²).
It runs vectorised in an **Arrow pandas UDF** with shapely 2, so there's no Sedona dependency, and it works
the same on Databricks.

**Python model and Spark must agree.** PV yield is computed in Spark SQL expressions (fast,
no UDF) and in the Python model (used by the API calculator). `test_spark_yield_matches_python_model`
asserts they match to 1e-9, so the map and the calculator can never disagree.

**Household economics as a lookup table, not a UDF on 343k rows.** Payback depends only on
(climate cell, consumption, size): 9 cells × 39 profiles × 5 sizes. That's computed on the driver
and joined. Cheaper, and you can inspect every number.

**Tariffs are data with effective dates (SCD Type 2).** `config/tariffs.yaml` → `gold.dim_tariff`
with `effective_from / effective_to / is_current`. A bill can be computed as of any date; the
2026-05 change (200 free units) didn't overwrite history. Unverified versions are flagged
and the UI shows the flag.

**Quality gate before publishing (write–audit–publish).** Eight critical checks are recorded in
`ops.dq_results`: bronze = silver + quarantine, quarantine rate < 5%, unique ids, every building
has climate data, 12 months per cell, yield within 1,100–1,900 kWh/kWp (India's physical range), and
gold row count = silver. Any failure raises and publish never runs.

**Serving: DuckDB snapshot + atomic symlink.** The API never reads Delta. Each passing run
writes `snapshots/<run_id>/solaratlas.duckdb`; `current` is swapped with `os.replace`, so readers
never see a half-written file, and the previous snapshot stays for a one-command rollback.
DuckDB rather than PostgreSQL because the data is read-only and analytical. It's a single file with no
server, and it uses less RAM on a 16 GB laptop.

**Own small DAG runner instead of Airflow locally.** Six stages with `--from` resume and a run
log in `ops.pipeline_runs`. Airflow would cost ~1.5 GB of RAM for six sequential steps. On
Databricks the same functions run as a Job (`databricks/job.yml`).

**Delta maintenance.** `./sa maintain` runs OPTIMIZE, Z-ORDER by (lon, lat) on the building tables
(bbox queries skip files), and VACUUM with 7 days of time travel kept.

**Spark tuned for a laptop.** `local[4]`, 2 GB driver (4 GB statewide, 1 GB for tests), 64 shuffle partitions
instead of 200, AQE on, UI off, every temp dir inside the project. Spark tests sit behind a marker
so the default `./sa test` never starts a JVM.

## Statewide scale (added week 2)

**Tiled, resumable extract.** The state bbox is cut into 65 non-overlapping 0.5° tiles. A building
belongs to the tile holding its bbox's south-west corner, so it lands exactly once with no dedupe
needed. Each tile is an atomic file + manifest (rows, sha256), so a dropped connection loses one
tile, not the run. The network throttles per connection, so three workers take disjoint tiles
(`--shard k/3`): throughput roughly tripled (~1 → ~3 tiles/min).

**Clip to the real state, not the box.** The bbox also covers parts of Kerala, Karnataka, Andhra
and Sri Lanka. Silver assigns every building to state, district, locality and land use with a
**broadcast STRtree join**: polygons (simplified to ~20 m) ride in the UDF closure, each Python worker
builds the tree once and answers a whole Arrow batch in one vectorised `query(predicate="within")`.
Out-of-state buildings are *counted* (`outside_region`), not quarantined, so the gate still
reconciles: bronze = silver + quarantine + outside.

**First load writes, later loads MERGE.** A MERGE joins source and target; on an empty target that is
pure overhead, so the first load of a region is a partition overwrite and later releases MERGE.

**Materialise the expensive step once.** The geometry and spatial UDFs feed three outputs (quarantine,
clean rows, counts). They are computed once into a `_tmp` Delta table instead of being recomputed by
each action.

**What broke at 25M rows, and the fix.** The first statewide Gold run failed with
`SparkOutOfMemoryError`: the four-way join (buildings with WKB geometry, type, features, climate) was
evaluated twice inside one write (once for the distinct lookup keys, once for the output), and 8
shuffle partitions meant ~600 MB per task. Fixes: materialise the joined base once to a `_tmp` Delta
table; 64 shuffle partitions (AQE coalesces them back for small regions); a larger execution-memory
fraction; and, for incremental runs, narrowing every input to the changed ids *before* joining.
Peak JVM resident memory, sampled every 2 s: 5.0 GB for a full statewide build with a 4 GB heap.

## Building type classifier

Only ~0.1% of footprints (25,377) carry a usable type tag. Spark ML random forest on the tagged ones (25k statewide):
features from shape (area, compactness, elongation, vertices), density (built-up share of the
H3 res-11 ring, buildings per res-9 cell), nearby places by group (583k Overture POIs) and land-use
zone. Classes are reweighted by inverse frequency; quality is reported as **macro-F1 and per-class
recall on a held-out 20%**, not just accuracy, because residential dominates. Tagged buildings keep
their tag (`type_source = osm`); others carry the model's probability. Caveat: tags are biased toward
well-mapped city areas.

Neighbourhood features use **H3 cells and equality joins** (`grid_disk` ring explode + group by),
not geometric joins: cheap at 30M rows and identical on Databricks.

## Shading

No heights (0.16% of footprints have one), so shading is a labelled heuristic: loss grows with
the built-up share of the ~150 m ring above 35%, capped at 15%, halved for big roofs. It is a
config value, so its sensitivity can be shown, and it is replaced when height data arrives.

## Economics by type, and scenarios

Homes: PM Surya Ghar subsidy, telescopic slabs, size 1–3 kW chosen for consumption estimated from
footprint (assumption, flagged). Shops, factories, institutions: no subsidy, flat energy rate,
most output self-consumed.

**Aggregate before join.** Homes with the same (district, climate cell, consumption, roof cap, shade
bucket) have identical economics, so 19.2M homes collapse to about 148,000 priced combinations. Each
policy scenario re-prices only those groups, not every home, so 5 scenarios do not need 5 full
passes. Scenarios are `config/scenarios.yaml` overrides applied through a scoped `config.overrides()`
context, so the model code has no scenario logic.

**Rounding can fake a result (found and fixed 2026-11-24).** Consumption was first rounded to 50 units,
so 957,452 homes shared one value and flipped between viable and not together. "Subsidy +33%" and
"cost −20%" then tied at exactly 3,729,004 homes and the published conclusion was wrong. The step is
now 1 unit. Steps of 1 and 2 agree within 0.6% (viable homes) and finer shade buckets move results
by under 0.1%. A test now requires the policies to have different thresholds.

## Hourly simulation and metering

Annual averages hide *when* solar is produced. With a year of hourly NASA POWER data per cell
(8,760 hours) and an assumed household load shape (evening peak, night-time fans/AC), each home's
self-use, export and import are simulated hour by hour. Bills are computed per two-month cycle
with the real telescopic tariff, under three rules: self-use only, net metering, net billing.
The hourly PV profile is computed in Spark (`silver.cell_hourly`, parity with the Python model) and
served to the API, which simulates any home in milliseconds.

## Incremental processing

Gold depends on three inputs: `silver.buildings`, `silver.building_type` and
`silver.building_features`. All three have Change Data Feed on, and the two recomputed every run
(features, model predictions) are written with a **change-aware MERGE** that only rewrites rows
whose values actually moved, so a retrained model that relabels 1,000 buildings produces 1,000 CDF
rows, not 25 million. Gold keeps one watermark per input (`ops.watermarks`). On the next run it reads only the changes since then, recomputes those
buildings, MERGEs them in and deletes removed ones. If silver hasn't changed, Gold does nothing.
`--full-refresh` forces a rebuild. Scenarios and summaries read Gold, so they stay correct after a
partial update.

## Databricks

`databricks.yml` is an Asset Bundle: a Unity Catalog schema + managed volume, and a 7-task job DAG
(bronze → silver → features → classify → gold → quality gate → maintain). Each task runs
`solaratlas/jobs/run_stage.py`, the same functions as the local runner, with `SA_DATA` pointing
at the volume. Tasks hand off only through Delta tables, so any task can be retried alone. Landing
files are made on the laptop (Free Edition limits outbound internet) and uploaded with
`scripts/upload_landing.sh`.

**Verified 2026-11-24 on Free Edition serverless, all of Tamil Nadu** (2.67 GB uploaded, every file size
checked). Task times: bronze 100 s, silver 373 s, features 94 s, classify 211 s, gold 317 s, quality 26 s,
maintain 311 s. After the split fix below, classify, gold and quality ran again. Compared with the laptop:

| | Laptop | Databricks |
|---|---|---|
| Buildings, roof area, technical potential, CO2, quarantined | – | identical |
| Viable homes | 2,879,992 | 2,879,064 (-0.03%) |
| Viable roofs | 3,151,922 | 3,140,698 (-0.36%) |
| Policy scenarios (viable homes) | – | within 0.06% |
| Classifier accuracy (same test buildings) | 81.1% | 81.3% |
| Institutional buildings | 116,313 | 100,113 (-14%) |

What serverless needed: environment 4 for Spark ML, project code pickled into UDFs, no `cache()`,
`_metadata.file_path` instead of `input_file_name()`, no reliance on `__file__`, retries off (daily
quota), and no bundle development mode (it renames the schema).

**A leak found by checking the numbers.** The first statewide run there reported 84.7% accuracy against
80.6% on the laptop. Train plus test rows came to 25,532 for 25,377 labels: `randomSplit(seed)` had been
evaluated on differently partitioned reads, so buildings sat on both sides. The split is now a hash of
the building id (same buildings on every machine, zero overlap verified) and the run fails if the counts
don't add up.

**Incremental at scale, measured.** Retraining changed 125,612 building types on Databricks (gold stage
264 s; the full rebuild before it took 317 s) and 102,837 on the laptop (gold stage 348 s; the last full
rebuild took 229 s). So on the laptop incremental was *slower* at this size: it rewrites fewer rows, but
its MERGE and the summaries that always recompute cost more than a plain partition overwrite.

## Model

- Usable roof: 40–65% of the footprint by size (setbacks, water tanks, stair rooms, shade).
- 10 m² per kWp (MNRE rule of thumb), performance ratio 0.77, −0.35%/°C above 25 °C cell
  temperature (cells run ~25 °C above ambient), 0.5%/yr degradation, 25-year life.
- Coimbatore: 1,408 kWh/kWp/yr, within the published 1,400–1,500 range for the region.
- Economics: ₹60,000/kWp turnkey (assumption), ₹500/kWp/yr O&M, PM Surya Ghar subsidy,
  sizes 1–3 kW. **Annual economics assume net metering within each two-month bill** (generation
  offsets that bill's consumption; surplus beyond it is worth zero). The hourly simulation shows
  the sensitivity: for a 1,200-unit Chennai home, self-use-only savings are ~39% lower.
  The recommender picks the size with the best 25-year net saving among sizes that pay back.

## Limitations (say these before an interviewer does)

1. No measured shading, tilt or orientation: footprints are 2D (shading is a crowding proxy, see 6). Usable fraction is a blanket assumption.
2. Climate cells are 0.5°: irradiance varies between districts, not within a city.
3. Building type is model-estimated for >99% of buildings (see classifier metrics). Home
   consumption is estimated from footprint size, not metered.
4. Tariff slabs come from secondary sources and are marked unverified until checked against
   the TNERC order. Capex is an assumption.
5. Technical potential (99.1 GW statewide) is an upper bound, not a forecast of adoption.
6. Shading is a crowding heuristic, not a 3D calculation; load shape and feed-in rate are assumptions.
7. Commercial/industrial tariffs are unverified (industrial ₹8/unit is an assumption).
8. The classifier is not bit-reproducible: the test buildings are now fixed, but the forest still depends
   on data layout. Laptop and Databricks builds differ by 14% in institutional counts and under 0.1% in
   viable homes.
9. Gold's watermarks track data, not config. After changing `config/`, run with `--full-refresh`.
