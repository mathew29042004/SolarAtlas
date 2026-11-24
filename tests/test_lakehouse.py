"""End-to-end lakehouse tests on synthetic landing data in a temp folder.

Run deliberately (starts a JVM):  pytest -m spark
"""
import json
import math
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely

pytestmark = pytest.mark.spark
FIX = Path(__file__).parent / "fixtures" / "power_cell.json"
REGION = "coimbatore_core"   # bbox 76.94-76.98, 11.00-11.03


def sq(lon, lat, side_m):
    dlat = side_m / 110_574
    dlon = side_m / (111_320 * math.cos(math.radians(lat)))
    return shapely.box(lon, lat, lon + dlon, lat + dlat)


def write_landing(landing: Path, release: str, rows: list[dict]):
    from solaratlas.ingest.buildings import landing_dir
    d = landing_dir(release, REGION)
    d.mkdir(parents=True, exist_ok=True)
    cols = ["id", "geometry_wkb", "xmin", "ymin", "xmax", "ymax", "height", "num_floors",
            "roof_shape", "roof_material", "class", "subtype", "source_dataset", "source_confidence"]
    data = {c: [r.get(c) for r in rows] for c in cols}
    schema = pa.schema([("id", pa.string()), ("geometry_wkb", pa.binary()), ("xmin", pa.float64()),
                        ("ymin", pa.float64()), ("xmax", pa.float64()), ("ymax", pa.float64()),
                        ("height", pa.float64()), ("num_floors", pa.int32()), ("roof_shape", pa.string()),
                        ("roof_material", pa.string()), ("class", pa.string()), ("subtype", pa.string()),
                        ("source_dataset", pa.string()), ("source_confidence", pa.float64())])
    pq.write_table(pa.table(data, schema=schema), d / "buildings.parquet")
    (d / "_manifest.json").write_text(json.dumps({"rows": len(rows)}))


def write_divisions(landing: Path, release: str):
    """Synthetic state / district / ward polygons around the test region."""
    from solaratlas.ingest.divisions import landing_dir
    d = landing_dir(release, "IN-TN")
    d.mkdir(parents=True, exist_ok=True)
    state = shapely.box(76.0, 10.0, 78.0, 12.0)
    ward = shapely.box(76.94, 11.00, 76.96, 11.03)
    rows = [("r", "d-r", "region", "Tamil Nadu", state), ("c", "d-c", "county", "Coimbatore", state),
            ("w", "d-w", "locality", "Ward 1", ward)]
    t = pa.table({"id": [r[0] for r in rows], "division_id": [r[1] for r in rows],
                  "level": [r[2] for r in rows], "name": [r[3] for r in rows],
                  "state_code": ["IN-TN"] * 3, "geometry_wkb": [r[4].wkb for r in rows]})
    pq.write_table(t, d / "division_area.parquet")


def b(id_, geom, conf=0.9, height=None):
    x0, y0, x1, y1 = geom.bounds if hasattr(geom, "bounds") else (0, 0, 0, 0)
    return {"id": id_, "geometry_wkb": geom.wkb if hasattr(geom, "wkb") else geom,
            "xmin": x0, "ymin": y0, "xmax": x1, "ymax": y1, "height": height,
            "source_dataset": "test", "source_confidence": conf}


GOOD = [b(f"g{i}", sq(76.95 + i * 0.001, 11.01, 8 + i * 3)) for i in range(20)]
BAD = [
    b("g0", sq(76.95, 11.01, 8), conf=0.1),                       # duplicate id, lower confidence
    b("bow", shapely.Polygon([(76.95, 11.02), (76.951, 11.021), (76.951, 11.02), (76.95, 11.021)])),
    b("huge", sq(76.945, 11.005, 300)),                           # 90,000 m2 > max footprint
    b("outside", sq(77.5, 11.5, 10)),                             # centroid outside region
    b("tall", sq(76.97, 11.02, 10), height=900),                  # implausible height
]


@pytest.fixture(scope="module")
def lake(tmp_path_factory):
    from solaratlas import config
    from solaratlas.lakehouse import spark as sp
    root = tmp_path_factory.mktemp("sa")
    old = (config.LANDING, config.LAKE, config.SERVING)
    config.LANDING, config.LAKE, config.SERVING = root / "landing", root / "lake", root / "serving"
    irr = config.LANDING / "irradiance" / "nasa_power_climatology"
    irr.mkdir(parents=True)
    shutil.copy(FIX, irr / "cell=77.00_11.00.json")
    release = config.load("sources")["overture_buildings"]["release"]
    write_divisions(config.LANDING, release)
    write_landing(config.LANDING, release, GOOD + BAD)
    yield config, release
    sp.stop()
    config.LANDING, config.LAKE, config.SERVING = old


def run_all(run_id, full=False):
    from solaratlas.lakehouse import bronze, classify, features, gold, silver
    bronze.buildings(REGION, run_id)
    bronze.irradiance(run_id)
    stats = silver.buildings(REGION, run_id)
    silver.places(REGION, run_id)
    silver.cell_climate(run_id)
    features.build(REGION, run_id)
    classify.train_and_predict(REGION, run_id)
    stats["gold"] = gold.building_solar(REGION, run_id, incremental=not full)
    gold.area_summaries(REGION, run_id)
    gold.scenarios(REGION, run_id)
    gold.household_economics(run_id)
    gold.dim_tariff(run_id)
    gold.region_summary(REGION, run_id)
    return stats


def read(config, layer, name):
    from solaratlas.lakehouse.spark import get
    return get().read.format("delta").load(config.table_path(layer, name))


def test_quarantine_reasons_and_reconciliation(lake):
    config, _ = lake
    stats = run_all("r1")
    assert stats["bronze_rows"] == len(GOOD) + len(BAD)
    assert stats["silver_rows"] == len(GOOD)
    assert stats["bronze_rows"] == stats["silver_rows"] + stats["quarantined"] + stats["outside_region"]
    assert stats["gold"]["mode"] == "full"
    row = read(config, "silver", "buildings").where("id = 'g1'").first()
    assert row.district == "Coimbatore" and row.locality == "Ward 1"
    reasons = {r.id: set(r.dq_reasons) for r in read(config, "silver", "buildings_quarantine").collect()}
    assert "duplicate_id" in reasons["g0"]
    assert "invalid_geometry" in reasons["bow"]
    assert "area_above_max" in reasons["huge"]
    assert "outside" not in reasons                 # outside the region: counted, not quarantined
    assert stats["outside_region"] == 1
    assert "implausible_height" in reasons["tall"]
    kept = read(config, "silver", "buildings").where("id = 'g0'").first()
    assert kept.source_confidence == 0.9          # the higher-confidence copy survived


def test_rerun_is_idempotent(lake):
    config, _ = lake
    from solaratlas.lakehouse import gold
    before = {t: read(config, layer, t).count() for layer, t in
              [("bronze", "buildings"), ("silver", "buildings"), ("gold", "building_solar")]}
    stats = run_all("r2")
    assert stats["gold"]["mode"] in ("noop", "incremental")
    assert gold.building_solar(REGION, "r2b")["mode"] == "noop"   # nothing changed since
    after = {t: read(config, layer, t).count() for layer, t in
             [("bronze", "buildings"), ("silver", "buildings"), ("gold", "building_solar")]}
    assert before == after


def test_spark_yield_matches_python_model(lake):
    from solaratlas.model import solar
    config, _ = lake
    p = json.loads(FIX.read_text())["properties"]["parameter"]
    expected = sum(solar.monthly_specific_yield(p["ALLSKY_SFC_SW_DWN"], p["T2M"]).values())
    got = read(config, "gold", "building_solar").first().annual_yield_kwh_kwp
    assert got == pytest.approx(expected, rel=1e-9)


def test_gold_numbers_follow_assumptions(lake):
    from solaratlas.model import solar
    config, _ = lake
    for r in read(config, "gold", "building_solar").collect():
        assert r.roof_kwp == pytest.approx(solar.roof_kwp(r.footprint_m2), abs=0.01)
        assert r.annual_kwh == pytest.approx(r.roof_kwp * r.annual_yield_kwh_kwp * (1 - r.shading_loss),
                                             rel=0.01, abs=1)
        assert r.eligible == (r.footprint_m2 >= 15)
        assert r.building_type in ("residential", "commercial", "industrial", "institutional")


def test_new_release_merges_updates_and_deletes(lake, monkeypatch):
    from solaratlas.lakehouse import bronze, silver
    config, release = lake
    new = "2099-01-01.0"
    sources = dict(config.load("sources"))
    sources["overture_buildings"] = {**sources["overture_buildings"], "release": new}
    monkeypatch.setattr(config, "load", lambda n, _o=config.load: sources if n == "sources" else _o(n))
    rows = [r for r in GOOD if r["id"] != "g19"]                  # g19 demolished
    rows.append(b("g20", sq(76.975, 11.025, 12)))                 # new building
    write_landing(config.LANDING, new, rows)
    bronze.buildings(REGION, "r3")
    stats = silver.buildings(REGION, "r3")
    ids = {r.id for r in read(config, "silver", "buildings").collect()}
    assert "g19" not in ids and "g20" in ids
    assert stats["silver_rows"] == len(rows)
    # unchanged buildings keep their old release stamp: only the new one carries the new release
    versions = read(config, "silver", "buildings")
    assert versions.where(f"release = '{new}'").count() == 1
    # gold picks up only what changed, through the Change Data Feed
    from solaratlas.lakehouse import classify, features, gold
    features.build(REGION, "r3")
    classify.train_and_predict(REGION, "r3")
    g = gold.building_solar(REGION, "r3")
    assert g["mode"] == "incremental"
    assert 0 < g["changed"] <= len(rows) + 1
    gids = {r.id for r in read(config, "gold", "building_solar").collect()}
    assert "g19" not in gids and "g20" in gids and len(gids) == len(rows)


def test_quality_gate_passes_and_fails(lake):
    from solaratlas.lakehouse import quality
    config, release = lake
    run_all("r4", full=True)
    stats = {"bronze_rows": len(GOOD) + len(BAD), "quarantined": len(BAD)}
    with pytest.raises(quality.QualityGateFailed):       # 5/25 quarantined = 20% > 5%
        quality.run(REGION, "r4", stats)
    stats_ok = {"bronze_rows": len(GOOD), "quarantined": 0}
    quality.run(REGION, "r5", stats_ok)


def test_units_expr_matches_python_model(lake):
    from solaratlas.lakehouse import gold
    from solaratlas.lakehouse.spark import get
    from solaratlas.model import solar
    areas = [10.0, 33.5, 100.0, 116.83333, 117.0, 117.5, 250.1666, 333.3, 5000.0]
    df = get().createDataFrame([(a,) for a in areas], "footprint_m2 double").withColumn("u", gold.units_expr())
    assert {r.footprint_m2: r.u for r in df.collect()} == {a: solar.estimated_units(a) for a in areas}


def test_classifier_split_is_disjoint_complete_and_order_independent(lake):
    from solaratlas.lakehouse.classify import split
    from solaratlas.lakehouse.spark import get
    df = get().createDataFrame([(f"b{i}",) for i in range(5000)], "id string")
    train, test = split(df)
    tr, te = ({r.id for r in x.collect()} for x in (train, test))
    assert not tr & te and len(tr) + len(te) == 5000
    assert 0.17 < len(te) / 5000 < 0.23
    _, te2 = split(df.orderBy(df.id.desc()).repartition(7))     # different order and layout
    assert {r.id for r in te2.collect()} == te
