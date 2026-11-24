"""Domestic electricity bill engine (telescopic slabs, bimonthly billing).

Tariffs are data, not code: each version in config/tariffs.yaml has an effective date range,
so a bill can be computed "as of" any date and history is never overwritten (SCD Type 2).
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from solaratlas import config


@dataclass(frozen=True)
class TariffVersion:
    scheme: str
    version: str
    effective_from: date
    effective_to: date | None
    free_units: int
    fixed_charge_inr: float
    switch_at_units: float
    low: tuple
    high: tuple
    verified: bool
    source: str


def versions(scheme: str = "tn_domestic") -> list[TariffVersion]:
    out = []
    for v in config.load("tariffs")[scheme]:
        out.append(TariffVersion(
            scheme=scheme, version=str(v["version"]),
            effective_from=_d(v["effective_from"]), effective_to=_d(v.get("effective_to")),
            free_units=v["free_units"], fixed_charge_inr=v["fixed_charge_inr"],
            switch_at_units=v["switch_at_units"],
            low=tuple((s["upto"], s["rate"]) for s in v["low"]),
            high=tuple((s["upto"], s["rate"]) for s in v["high"]),
            verified=bool(v.get("verified")), source=v["source"].strip(),
        ))
    return sorted(out, key=lambda t: t.effective_from)


def _d(x):
    if x is None or isinstance(x, date):
        return x
    return date.fromisoformat(str(x))


def as_of(scheme: str = "tn_domestic", on: date | None = None) -> TariffVersion:
    forced = config.override_value("tariff.version")
    if forced:  # scenario: apply a specific tariff version's rules
        for t in versions(scheme):
            if t.version == str(forced):
                return t
        raise LookupError(f"no {scheme} tariff version {forced}")
    on = on or date.today()
    for t in versions(scheme):
        if t.effective_from <= on and (t.effective_to is None or on <= t.effective_to):
            return t
    raise LookupError(f"no {scheme} tariff effective on {on}")


def bill(units: float, tariff: TariffVersion) -> float:
    """Energy + fixed charge in INR for one bimonthly cycle of `units` kWh."""
    if units <= 0:
        return 0.0
    slabs = tariff.low if units <= tariff.switch_at_units else tariff.high
    total, lower = 0.0, 0.0
    for upto, rate in slabs:
        top = units if upto is None else min(units, upto)
        if top > lower:
            total += (top - lower) * rate
        if upto is None or units <= upto:
            break
        lower = upto
    return round(total + tariff.fixed_charge_inr, 2)
