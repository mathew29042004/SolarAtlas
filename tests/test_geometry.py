import math

import pandas as pd
import pytest
import shapely

from solaratlas.ingest.irradiance import cell_id, lattice
from solaratlas.lakehouse.silver import footprint_metrics


def square(lon, lat, side_m):
    dlat = side_m / 110_574
    dlon = side_m / (111_320 * math.cos(math.radians(lat)))
    return shapely.box(lon, lat, lon + dlon, lat + dlat)


def test_footprint_area_in_square_metres():
    g = [square(76.96, 11.0, 10).wkb, square(76.96, 11.0, 40).wkb]
    out = footprint_metrics(pd.Series(g))
    assert out.footprint_m2.tolist() == pytest.approx([100, 1600], rel=2e-3)
    assert out.geom_valid.all()


def test_invalid_and_garbage_geometry_flagged():
    bowtie = shapely.Polygon([(0, 0), (1, 1), (1, 0), (0, 1)]).wkb
    out = footprint_metrics(pd.Series([bowtie, b"not wkb"]))
    assert not out.geom_valid.any()


def test_cell_id_snaps_to_lattice():
    assert cell_id(76.96, 11.01) == "77.00_11.00"
    assert cell_id(76.70, 10.70) == "76.50_10.50"
    assert cell_id(80.27, 13.08) == "80.50_13.00"


def test_lattice_covers_bbox():
    pts = lattice((76.88, 10.93, 77.06, 11.08))
    lons = {p[0] for p in pts}
    lats = {p[1] for p in pts}
    assert min(lons) <= 76.88 and max(lons) >= 77.06
    assert min(lats) <= 10.93 and max(lats) >= 11.08


def test_shape_metrics():
    out = footprint_metrics(pd.Series([square(76.96, 11.0, 10).wkb,
                                       shapely.box(76.96, 11.0, 76.96 + 40 / 109_270, 11.0 + 10 / 110_574).wkb]))
    assert out.perimeter_m[0] == pytest.approx(40, rel=3e-3)
    assert out.compactness[0] == pytest.approx(math.pi / 4, rel=1e-2)
    assert out.elongation[0] == pytest.approx(1, rel=1e-2)
    assert out.elongation[1] == pytest.approx(4, rel=2e-2)


def test_nested_polygons_pick_smallest():
    import numpy as np

    from solaratlas.lakehouse import spatial
    metro = shapely.box(0, 0, 10, 10)
    ward = shapely.box(1, 1, 2, 2)
    polys = [("m", "Metro", metro.wkb), ("w", "Ward 1", ward.wkb)]
    names, _ = spatial.lookup("test-nested", polys, np.array([1.5, 5.0]), np.array([1.5, 5.0]), smallest=True)
    assert list(names) == ["Ward 1", "Metro"]
