"""Hourly self-consumption and metering simulation for one home.

Inputs are a year of hourly PV output per kWp (from NASA POWER hourly irradiance and temperature)
and a load profile built from the household's bimonthly units. For each hour:
    self-used = min(pv, load),  export = pv - self-used,  import = load - self-used
Bills are computed per two-month cycle with the real telescopic tariff, so the 500-unit cliff and
free units apply exactly as on a TNPDCL bill. Three regimes are compared (config/metering.yaml).
"""
from __future__ import annotations

from datetime import datetime

import numpy as np

from solaratlas import config
from solaratlas.model import tariff as tariffs


def M() -> dict:
    return config.load("metering")


def pv_per_kwp(ghi_wh_m2: np.ndarray, t2m_c: np.ndarray) -> np.ndarray:
    """kWh per kWp for each hour. Hourly GHI in Wh/m2 equals mean W/m2 over that hour."""
    pv = config.load("assumptions")["pv"]
    g = np.clip(ghi_wh_m2, 0, None)
    t_cell = t2m_c + M()["cell_temp_coeff_c_per_wm2"] * g
    return g / 1000 * pv["performance_ratio"] * (1 + pv["temp_coeff_per_c"] * (t_cell - 25))


def parse_power_hourly(parameter: dict) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """POWER hourly JSON -> (months 1..12, hour 0..23, pv kWh/kWp), missing values as 0."""
    keys = sorted(parameter["ALLSKY_SFC_SW_DWN"])
    ghi = np.array([parameter["ALLSKY_SFC_SW_DWN"][k] for k in keys], dtype=float)
    t2m = np.array([parameter["T2M"][k] for k in keys], dtype=float)
    ghi[ghi < 0] = 0          # -999 fill values
    t2m[t2m < -50] = 25
    months = np.array([int(k[4:6]) for k in keys])
    hours = np.array([int(k[8:10]) for k in keys])
    return months, hours, pv_per_kwp(ghi, t2m)


def load_profile(units_bimonthly: float, months: np.ndarray, hours: np.ndarray) -> np.ndarray:
    """Hourly kWh for a home using `units_bimonthly` on average per two-month bill."""
    m = M()
    shape = np.asarray(m["load_shape_24h"], dtype=float)
    shape = shape / shape.sum()
    mf = np.asarray(m["monthly_factor"], dtype=float)[months - 1]
    mf = mf / mf.mean()                        # weighted by actual hours, so the year sums exactly
    daily = units_bimonthly * 6 / 365          # average kWh/day over the year
    return daily * shape[hours] * mf


def _cycles(months: np.ndarray) -> np.ndarray:
    return (months - 1) // 2                  # 0..5: Jan-Feb, Mar-Apr, ...


def simulate(months, hours, pv_kwh_per_kwp, kwp: float, units_bimonthly: float,
             tariff: tariffs.TariffVersion | None = None) -> dict:
    tariff = tariff or tariffs.as_of()
    om = config.load("assumptions")["economics"]["om_inr_per_kwp_year"] * kwp
    load = load_profile(units_bimonthly, months, hours)
    pv = pv_kwh_per_kwp * kwp
    self_used = np.minimum(pv, load)
    export = pv - self_used
    imp = load - self_used
    cyc = _cycles(months)
    regimes = M()["regimes"]
    bills_before, out = [], {}
    per_cycle = []
    for c in range(6):
        sel = cyc == c
        per_cycle.append((load[sel].sum(), imp[sel].sum(), export[sel].sum()))
        bills_before.append(tariffs.bill(load[sel].sum(), tariff))
    before = sum(bills_before)
    # self-consumption only: pay for imports, export worth nothing
    sc = sum(tariffs.bill(i, tariff) for _, i, _ in per_cycle)
    # net metering: export offsets imports within the cycle; surplus carried forward, year-end paid
    carry, nm = 0.0, 0.0
    for _, i, e in per_cycle:
        net = i - e - carry
        if net >= 0:
            nm += tariffs.bill(net, tariff)
            carry = 0.0
        else:
            carry = -net
    nm -= carry * regimes["net_metering"]["feed_in_inr"]
    # net billing: imports billed, every exported kWh paid feed-in
    nb = sc - export.sum() * regimes["net_billing"]["feed_in_inr"]
    for key, after in (("self_consumption", sc), ("net_metering", nm), ("net_billing", nb)):
        out[key] = {"label": regimes[key]["label"], "bill_after_inr": round(after),
                    "annual_saving_inr": round(before - after - om)}
    day = np.zeros((24, 2))
    for h in range(24):
        sel = hours == h
        day[h] = [pv[sel].mean(), load[sel].mean()]
    return {
        "kwp": kwp, "units_bimonthly": units_bimonthly, "tariff_version": tariff.version,
        "annual_pv_kwh": round(float(pv.sum())), "annual_load_kwh": round(float(load.sum())),
        "self_used_kwh": round(float(self_used.sum())), "export_kwh": round(float(export.sum())),
        "import_kwh": round(float(imp.sum())),
        "self_consumption_ratio": round(float(self_used.sum() / pv.sum()), 3) if pv.sum() else 0,
        "self_sufficiency": round(float(self_used.sum() / load.sum()), 3) if load.sum() else 0,
        "bill_before_inr": round(before), "regimes": out,
        "typical_day": [{"hour": h, "pv_kwh": round(float(day[h, 0]), 3), "load_kwh": round(float(day[h, 1]), 3)}
                        for h in range(24)],
        "simulated_at": datetime.now().isoformat(timespec="seconds"),
    }
