import json
from pathlib import Path

import pytest

from solaratlas.model import solar
from solaratlas.model import tariff as t

FIX = json.loads((Path(__file__).parent / "fixtures" / "power_cell.json").read_text())
P = FIX["properties"]["parameter"]


@pytest.mark.parametrize("kwp,inr", [(1, 30000), (2, 60000), (2.5, 69000), (3, 78000), (10, 78000)])
def test_pm_surya_ghar_subsidy(kwp, inr):
    assert solar.subsidy_inr(kwp) == inr


def test_usable_fraction_bands():
    assert solar.usable_fraction(30) == 0.40
    assert solar.usable_fraction(100) == 0.55
    assert solar.usable_fraction(10_000) == 0.65


def test_roof_kwp_threshold_and_scale():
    assert solar.roof_kwp(10) == 0
    assert solar.roof_kwp(100) == pytest.approx(100 * 0.55 / 10)


def test_specific_yield_is_plausible_for_coimbatore():
    y = solar.monthly_specific_yield(P["ALLSKY_SFC_SW_DWN"], P["T2M"])
    assert set(y) == set(solar.MONTHS)
    assert 1300 < sum(y.values()) < 1550           # published range for south India ~1400-1500
    assert y["MAR"] > y["JUL"]                     # monsoon dip


def test_economics_reflect_tariff_cliff():
    tv = t.versions()[-1]
    small = solar.household_economics(1, 1408, 300, tv)
    big = solar.household_economics(3, 1408, 1000, tv)
    assert big["payback_years"] < 5
    assert small["payback_years"] is None or small["payback_years"] > big["payback_years"]
    assert big["net_cost_inr"] == 3 * 60000 - 78000


def test_assumptions_are_numeric():
    """Guards against YAML quirks (e.g. `1.0e9` parses as a string in YAML 1.1)."""
    a = solar.A()
    for band in a["roof"]["usable_fraction_by_size"]:
        assert isinstance(band["max_m2"], int | float) and isinstance(band["fraction"], float)
    for section in ("pv", "economics", "grid"):
        for k, v in a[section].items():
            vals = v if isinstance(v, list) else [v]
            assert all(isinstance(x, int | float) for x in vals), f"{section}.{k} = {v!r}"


def test_best_size_respects_options_and_roof():
    tv = t.versions()[-1]
    best, options = solar.best_size(1408, 600, tv)
    assert {o["kwp"] for o in options} == set(solar.size_options())
    assert min(solar.size_options()) >= 1
    roof_best, _ = solar.best_size(1408, 1500, tv, max_kwp=1.7)
    assert roof_best["kwp"] <= 1.7
    tiny, opts = solar.best_size(1408, 600, tv, max_kwp=0.6)
    assert tiny["kwp"] == 0.6 and len(opts) == 1


def test_scenario_overrides_are_scoped():
    from solaratlas import config
    base = solar.subsidy_inr(3)
    with config.overrides({"subsidy.pm_surya_ghar.cap_inr": 0}):
        assert solar.subsidy_inr(3) == 0
    assert solar.subsidy_inr(3) == base
    with config.overrides({"tariff.version": "2024-07"}):
        assert t.as_of().free_units == 100
    with pytest.raises(KeyError):
        with config.overrides({"subsidy.pm_surya_ghar.typo": 1}):
            solar.subsidy_inr(1)
