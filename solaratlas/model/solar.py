"""PV yield, subsidy and household economics. Pure functions over config/assumptions.yaml."""
from __future__ import annotations

import math

from solaratlas import config
from solaratlas.model import tariff as tariffs

DAYS = {"JAN": 31, "FEB": 28.25, "MAR": 31, "APR": 30, "MAY": 31, "JUN": 30,
        "JUL": 31, "AUG": 31, "SEP": 30, "OCT": 31, "NOV": 30, "DEC": 31}
MONTHS = list(DAYS)


def A() -> dict:
    return config.load("assumptions")


def usable_fraction(footprint_m2: float) -> float:
    for band in A()["roof"]["usable_fraction_by_size"]:
        if footprint_m2 <= band["max_m2"]:
            return band["fraction"]
    return A()["roof"]["usable_fraction_by_size"][-1]["fraction"]


def roof_kwp(footprint_m2: float) -> float:
    r = A()["roof"]
    if footprint_m2 < r["min_footprint_m2"]:
        return 0.0
    return footprint_m2 * usable_fraction(footprint_m2) / r["m2_per_kwp"]


def monthly_specific_yield(ghi_kwh_m2_day: dict, t2m_c: dict) -> dict:
    """kWh produced per kWp in each month (first year).

    yield = GHI[kWh/m2/day] * days * PR * temperature factor. GHI in kWh/m2/day equals
    "peak sun hours", and 1 kWp is rated at 1 kW/m2, so GHI * days is kWh per kWp before losses.
    Panels lose ~0.35%/degC above 25 degC cell temperature; cells run ~25 degC above ambient.
    """
    pv = A()["pv"]
    out = {}
    for m in MONTHS:
        t_cell = t2m_c[m] + pv["cell_temp_rise_c"]
        temp_factor = 1 + pv["temp_coeff_per_c"] * (t_cell - 25)
        out[m] = ghi_kwh_m2_day[m] * DAYS[m] * pv["performance_ratio"] * temp_factor
    return out


def size_options() -> list[float]:
    return [float(k) for k in A()["economics"]["size_options_kwp"]]


def best_size(annual_kwh_per_kwp: float, bimonthly_units: float, tariff: tariffs.TariffVersion,
              max_kwp: float | None = None) -> tuple[dict, list[dict]]:
    """Size that maximises 25-year net saving (among sizes that pay back), within the roof limit."""
    sizes = [k for k in size_options() if max_kwp is None or k <= max_kwp]
    if not sizes and max_kwp:
        sizes = [round(max_kwp, 2)]
    options = [household_economics(k, annual_kwh_per_kwp, bimonthly_units, tariff) for k in sizes]
    viable = [o for o in options if o["payback_years"] is not None]
    best = max(viable, key=lambda o: o["lifetime_saving_inr"]) if viable else options[0]
    return best, options


def subsidy_inr(kwp: float) -> float:
    s = config.load("subsidy")["pm_surya_ghar"]
    total, lower = 0.0, 0.0
    for slab in s["slabs"]:
        part = max(0.0, min(kwp, slab["upto_kwp"]) - lower)
        total += part * slab["inr_per_kwp"]
        lower = slab["upto_kwp"]
    return min(total, s["cap_inr"])


def household_economics(kwp: float, annual_kwh_per_kwp: float, bimonthly_units: float,
                        tariff: tariffs.TariffVersion) -> dict:
    """First-year savings and simple payback for a home, assuming NET METERING per bill.

    Each two-month cycle's generation offsets that cycle's consumption (so daytime export counts
    against evening imports); any surplus beyond the cycle's consumption is valued at zero.
    Generation is spread evenly over the six cycles. The hourly model (model/hourly.py) shows
    how much lower savings are without net metering.
    """
    e = A()["economics"]
    pv = A()["pv"]
    gen_cycle = kwp * annual_kwh_per_kwp / 6
    before = tariffs.bill(bimonthly_units, tariff)
    after = tariffs.bill(max(bimonthly_units - gen_cycle, 0.0), tariff)
    annual_saving = 6 * (before - after) - e["om_inr_per_kwp_year"] * kwp
    capex = kwp * e["capex_inr_per_kwp"]
    net_cost = capex - subsidy_inr(kwp)
    # lifetime savings with degradation (tariff held flat: conservative)
    life = sum(annual_saving * (1 - pv["degradation_per_year"]) ** y
               for y in range(pv["lifetime_years"]))
    return {
        "kwp": round(kwp, 2),
        "bill_before_inr": before,
        "bill_after_inr": after,
        "annual_saving_inr": round(annual_saving, 0),
        "capex_inr": round(capex, 0),
        "subsidy_inr": round(subsidy_inr(kwp), 0),
        "net_cost_inr": round(net_cost, 0),
        "payback_years": round(net_cost / annual_saving, 1) if annual_saving > 0 else None,
        "lifetime_saving_inr": round(life - net_cost, 0),
        "tariff_version": tariff.version,
    }


def estimated_units(footprint_m2: float) -> int:
    """Bimonthly consumption guess for a home from its footprint (assumption)."""
    r = A()["residential"]
    u = min(max(footprint_m2 * r["units_per_m2_bimonthly"], r["min_units"]), r["max_units"])
    step = r["units_step"]
    return int(math.floor(u / step + 0.5) * step)   # half-up, like the Spark twin


def shading_loss(built_frac_ring: float, footprint_m2: float) -> float:
    """Crowding proxy for shade from neighbours (no heights available). See config shading."""
    s = A()["shading"]
    loss = max(0.0, built_frac_ring - s["free_below_built_fraction"]) * s["loss_per_unit_fraction"]
    if footprint_m2 >= s["large_roof_m2"]:
        loss /= 2
    return min(loss, s["max_loss"])


def nondomestic_economics(category: str, roof_kwp: float, annual_kwh_per_kwp: float) -> dict:
    """Shops, factories, institutions: daytime load, self-consumed share offsets the energy rate."""
    c = config.load("nondomestic")["categories"][category]
    e = A()["economics"]
    kwp = min(roof_kwp, c["cap_kwp"])
    gen = kwp * annual_kwh_per_kwp
    saving = gen * c["self_consumption"] * c["energy_rate_inr"] - e["om_inr_per_kwp_year"] * kwp
    capex = kwp * c["capex_inr_per_kwp"]
    return {"kwp": round(kwp, 2), "annual_kwh": round(gen), "annual_saving_inr": round(saving),
            "capex_inr": round(capex), "subsidy_inr": 0,
            "payback_years": round(capex / saving, 1) if saving > 0 else None}
