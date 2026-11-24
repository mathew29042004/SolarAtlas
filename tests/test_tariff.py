from datetime import date

import pytest

from solaratlas.model import tariff as t


@pytest.fixture
def v2026():
    return t.as_of(on=date(2026, 11, 1))


def test_version_lookup_is_date_effective():
    assert t.as_of(on=date(2025, 1, 1)).version == "2024-07"
    assert t.as_of(on=date(2026, 5, 9)).version == "2024-07"
    assert t.as_of(on=date(2026, 5, 10)).version == "2026-05"
    with pytest.raises(LookupError):
        t.as_of(on=date(2020, 1, 1))


def test_versions_do_not_overlap():
    vs = t.versions()
    for a, b in zip(vs, vs[1:], strict=False):
        assert a.effective_to is not None and a.effective_to < b.effective_from


@pytest.mark.parametrize("units,expected", [
    (0, 0.0), (150, 0.0), (200, 0.0),
    (300, 100 * 4.70),
    (500, 200 * 4.70 + 100 * 6.30),
])
def test_low_band_telescopic(v2026, units, expected):
    assert t.bill(units, v2026) == pytest.approx(expected)


def test_crossing_500_switches_whole_bill_to_high_band(v2026):
    # 501 units: 100 free, 101-400 @4.70, 401-500 @6.30, 501 @8.40
    assert t.bill(501, v2026) == pytest.approx(300 * 4.70 + 100 * 6.30 + 1 * 8.40)
    assert t.bill(501, v2026) - t.bill(500, v2026) > 400   # the cliff solar helps you avoid


def test_open_ended_top_slab(v2026):
    expected = 300 * 4.70 + 100 * 6.30 + 100 * 8.40 + 200 * 9.45 + 200 * 10.50 + 200 * 11.55
    assert t.bill(1200, v2026) == pytest.approx(expected)


def test_bill_is_monotonic(v2026):
    bills = [t.bill(u, v2026) for u in range(0, 2001, 10)]
    assert bills == sorted(bills)
