import numpy as np
import pytest

from solaratlas.model import hourly
from solaratlas.model import tariff as t

HOURS = np.tile(np.arange(24), 365)
MONTHS = np.repeat(np.array([m for m, d in zip(range(1, 13), [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31],
                                               strict=True) for _ in range(d)]), 24)
# idealised clear sky: bell curve 6:00-18:00 peaking at 1 kWh/kWp at noon
PV = np.where((HOURS >= 6) & (HOURS <= 18), np.sin((HOURS - 6) / 12 * np.pi), 0.0) * 0.8


def test_load_profile_matches_bill():
    load = hourly.load_profile(600, MONTHS, HOURS)
    assert load.sum() == pytest.approx(600 * 6 * 365 / 365, rel=1e-6)


def test_energy_balance():
    r = hourly.simulate(MONTHS, HOURS, PV, 2.0, 600, t.versions()[-1])
    assert r["self_used_kwh"] + r["export_kwh"] == pytest.approx(r["annual_pv_kwh"], abs=2)
    assert r["self_used_kwh"] + r["import_kwh"] == pytest.approx(r["annual_load_kwh"], abs=2)
    assert 0 < r["self_consumption_ratio"] < 1


def test_net_metering_beats_self_use():
    """Daytime surplus offsets evening use under net metering; with self-use only it is lost."""
    r = hourly.simulate(MONTHS, HOURS, PV, 3.0, 800, t.versions()[-1])
    s = {k: v["annual_saving_inr"] for k, v in r["regimes"].items()}
    assert s["net_metering"] >= s["net_billing"] >= s["self_consumption"]


def test_power_parsing_handles_fill_values():
    p = {"ALLSKY_SFC_SW_DWN": {"2025010112": 800.0, "2025010113": -999.0},
         "T2M": {"2025010112": 30.0, "2025010113": -999.0}}
    months, hours, pv = hourly.parse_power_hourly(p)
    assert list(hours) == [12, 13] and pv[1] == 0 and 0.55 < pv[0] < 0.62
