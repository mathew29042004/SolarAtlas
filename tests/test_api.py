"""API tests against a tiny, self-contained DuckDB snapshot (no Spark needed)."""
import duckdb
import h3
import pytest
import shapely
from fastapi.testclient import TestClient

R = "tn_test"


def bld(con, id_, lon, lat, m2, kwp, btype, viable, payback, units):
    sq = shapely.box(lon, lat, lon + 0.0002, lat + 0.0002).wkb
    con.execute("INSERT INTO building_solar VALUES (?, ?, 'Coimbatore', 'Ward 41', ?, ?, ?, '77.00_11.00', ?, ?, "
                "?, ?, ?, ?, ?, 'osm', 1.0, 'small_home', 'OpenStreetMap', ?, 0.02, ?, ?, 1408, 't')",
                [id_, R, lon, lat, sq, m2, kwp, kwp * 1380, min(kwp, 3), payback, viable, btype, m2 * 0.6,
                 kwp * 0.98, units])


@pytest.fixture(scope="module")
def client(tmp_path_factory):
    db = tmp_path_factory.mktemp("serving") / "solaratlas.duckdb"
    con = duckdb.connect(str(db))
    con.execute("""CREATE TABLE building_solar(id VARCHAR, region VARCHAR, district VARCHAR, locality VARCHAR,
        lon DOUBLE, lat DOUBLE, geometry_wkb BLOB, cell_id VARCHAR, footprint_m2 DOUBLE, roof_kwp DOUBLE,
        annual_kwh DOUBLE, system_kwp DOUBLE, payback_years DOUBLE, viable BOOLEAN, building_type VARCHAR,
        type_source VARCHAR, type_prob DOUBLE, size_class VARCHAR, source_dataset VARCHAR,
        usable_roof_m2 DOUBLE, shading_loss DOUBLE, co2_t_per_year DOUBLE, est_units INT,
        annual_yield_kwh_kwp DOUBLE, _run_id VARCHAR)""")
    bld(con, "home", 76.9501, 11.0001, 120, 6.6, "residential", True, 4.2, 350)
    bld(con, "shed", 76.9505, 11.0005, 12, 0.0, "residential", False, None, 100)
    bld(con, "mill", 76.9510, 11.0010, 2000, 130.0, "industrial", True, 3.9, None)
    con.execute("ALTER TABLE building_solar ADD COLUMN capex_inr DOUBLE DEFAULT 100000")
    con.execute("ALTER TABLE building_solar ADD COLUMN annual_saving_inr DOUBLE DEFAULT 25000")
    con.execute(f"""CREATE TABLE region_summary AS SELECT '{R}' AS region, 'Test TN' AS name,
        'Tamil Nadu' AS state, 3 AS buildings, 2 AS eligible_buildings, 136.6 AS potential_mwp,
        1 AS districts, 't' AS _run_id""")
    con.execute(f"""CREATE TABLE district_summary AS SELECT '{R}' AS region, 'Coimbatore' AS district,
        3 AS buildings, 0.14 AS potential_mwp, 0.19 AS potential_gwh_year, 76.95 AS lon, 11.0 AS lat,
        1 AS rank_potential, 't' AS _run_id""")
    con.execute(f"""CREATE TABLE locality_summary AS SELECT '{R}' AS region, 'Coimbatore' AS district,
        'Ward 41' AS locality, 3 AS buildings, 0.14 AS potential_mwp, 0.19 AS potential_gwh_year,
        76.95 AS lon, 11.0 AS lat, 1 AS rank_in_district, 't' AS _run_id""")
    con.execute(f"""CREATE TABLE scenario_district AS SELECT * FROM (VALUES
        ('baseline', 'Current policy', 'Coimbatore', 100, 40, 5, 1.0, 2.0, 10.0, '{R}', 't'),
        ('subsidy_up', 'Subsidy up', 'Coimbatore', 100, 50, 5, 1.2, 3.0, 12.0, '{R}', 't'))
        t(scenario, label, district, homes, viable_homes, viable_nondomestic, viable_mwp, subsidy_cr,
          investment_cr, region, _run_id)""")
    c7 = h3.latlng_to_cell(11.0001, 76.9501, 7)
    con.execute(f"""CREATE TABLE hex_summary AS SELECT '{R}' AS region, '{c7}' AS h3, 7 AS res, 3 AS buildings,
        0.14 AS potential_mwp, 0.19 AS potential_gwh_year, 't' AS _run_id""")
    con.execute("""CREATE TABLE household_economics AS SELECT '77.00_11.00' AS cell_id, 600 AS bimonthly_units,
        2.0 AS best_kwp, 16280.0 AS annual_saving_inr, 3.7 AS payback_years, true AS worth_it""")
    con.execute("CREATE TABLE dim_tariff AS SELECT DATE '2026-05-10' AS effective_from, 'low' AS band, "
                "0.0 AS from_units, 't' AS _run_id")
    con.execute("CREATE TABLE dq_results AS SELECT 'silver_not_empty' AS \"check\", 'critical' AS severity, "
                "true AS passed, '3' AS observed, '> 0' AS expected, 'r1' AS run_id")
    con.execute("CREATE TABLE _meta AS SELECT 'test-run' AS run_id, now() AS published_at")
    con.close()
    import api.app as appmod
    appmod.DB = db
    appmod._local.__dict__.clear()
    appmod.default_region = lambda: R
    return TestClient(appmod.app)


def test_health(client):
    assert client.get("/health").json()["run_id"] == "test-run"


def test_summary_by_type(client):
    s = client.get("/api/summary").json()
    assert s["buildings"] == 3
    assert {r["building_type"] for r in s["by_type"]} == {"residential", "industrial"}


def test_unknown_region_404(client):
    assert client.get("/api/summary?region=mars").status_code == 404


def test_rankings(client):
    assert client.get("/api/districts").json()[0]["district"] == "Coimbatore"
    assert client.get("/api/localities?district=Coimbatore").json()[0]["locality"] == "Ward 41"


def test_scenarios_delta_and_cost_per_home(client):
    s = {r["scenario"]: r for r in client.get("/api/scenarios").json()}
    assert s["subsidy_up"]["delta_viable_homes"] == 10
    assert s["subsidy_up"]["subsidy_per_extra_home_inr"] == 1_000_000   # 1 crore / 10 homes
    assert list(s)[0] == "baseline"


def test_hexes_closed_polygons(client):
    ring = client.get("/api/hexes?res=7").json()["features"][0]["geometry"]["coordinates"][0]
    assert ring[0] == ring[-1] and len(ring) == 7


def test_buildings_bbox_and_validation(client):
    r = client.get("/api/buildings?bbox=76.94,10.99,76.96,11.01").json()
    assert {f["properties"]["id"] for f in r["features"]} == {"home", "shed", "mill"}
    assert client.get("/api/buildings?bbox=1,2,3").status_code == 422
    assert client.get("/api/buildings?bbox=76,10,78,12").status_code == 413


def test_building_detail_by_type(client):
    home = client.get("/api/buildings/home").json()
    assert home["household"]["units_bimonthly"] == 350     # estimated consumption used by default
    assert home["household"]["recommended"]["kwp"] <= 3
    assert client.get("/api/buildings/home?units=1000").json()["household"]["units_bimonthly"] == 1000
    mill = client.get("/api/buildings/mill").json()
    assert mill["business"]["subsidy_inr"] == 0 and mill["business"]["kwp"] <= 150
    assert client.get("/api/buildings/shed").json()["household"] is None


def test_report_page(client):
    r = client.get("/report/home")
    assert r.status_code == 200 and "PM Surya Ghar" in r.text and "Ward 41" in r.text
    assert "this industrial building" in client.get("/report/mill").text


def test_top_roofs_and_search(client):
    top = client.get("/api/top-roofs?building_type=industrial").json()
    assert [t["id"] for t in top] == ["mill"]
    kinds = {r["kind"] for r in client.get("/api/search?q=coimb").json()}
    assert "district" in kinds
    assert client.get("/api/search?q=ward").json()[0]["name"] == "Ward 41"


def test_calculator_cliff(client):
    low = client.get("/api/calculator?units=300").json()
    high = client.get("/api/calculator?units=1000").json()
    assert high["worth_it"] and high["recommended"]["payback_years"] < 5
    assert (low["recommended"]["payback_years"] or 99) > high["recommended"]["payback_years"]


def test_ops(client):
    o = client.get("/api/ops").json()
    assert o["quality"][0]["check"] == "silver_not_empty"
