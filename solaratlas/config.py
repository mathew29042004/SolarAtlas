"""Loads the YAML configs and resolves project paths in one place."""
from __future__ import annotations

import copy
import os
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import yaml

ROOT = Path(os.environ.get("SA_HOME", Path(__file__).resolve().parents[1]))
CONFIG = ROOT / "config"
# On Databricks, SA_DATA points at a Unity Catalog volume, e.g. /Volumes/solaratlas/core/data
DATA = Path(os.environ.get("SA_DATA", ROOT / "data"))
LANDING = DATA / "landing"      # immutable raw files exactly as downloaded
LAKE = DATA / "lake"            # Delta tables: bronze / silver / gold
SERVING = DATA / "serving"      # parquet snapshots + DuckDB file the API reads


_overrides: ContextVar[dict | None] = ContextVar("overrides", default=None)


@lru_cache
def _raw(name: str) -> dict:
    with open(CONFIG / f"{name}.yaml") as f:
        return yaml.safe_load(f)


def load(name: str) -> dict:
    """Config file as a dict, with any active scenario overrides applied."""
    ov = {k[len(name) + 1:]: v for k, v in (_overrides.get() or {}).items() if k.startswith(name + ".")}
    if not ov:
        return _raw(name)
    out = copy.deepcopy(_raw(name))
    for path, value in ov.items():
        node = out
        *parents, leaf = path.split(".")
        for key in parents:
            node = node[key]
        if leaf not in node:
            raise KeyError(f"override {name}.{path}: unknown key {leaf!r}")
        node[leaf] = value
    return out


def override_value(key: str, default=None):
    """Non-file overrides such as `tariff.version`."""
    return (_overrides.get() or {}).get(key, default)


@contextmanager
def overrides(mapping: dict):
    """Temporarily apply dotted-path overrides, e.g. {"subsidy.pm_surya_ghar.cap_inr": 0}."""
    token = _overrides.set({**(_overrides.get() or {}), **mapping})
    try:
        yield
    finally:
        _overrides.reset(token)


@dataclass(frozen=True)
class Region:
    key: str
    name: str
    state: str
    tariff: str
    bbox: tuple[float, float, float, float]


def region(key: str | None = None) -> Region:
    cfg = load("regions")
    key = key or cfg["default"]
    r = cfg["regions"][key]
    return Region(key, r["name"], r["state"], r["tariff"], tuple(r["bbox"]))


def table_path(layer: str, name: str) -> str:
    return str(LAKE / layer / name)
