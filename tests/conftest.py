import pytest


def pytest_collection_modifyitems(config, items):
    """Spark tests start a JVM; run them only with `-m spark` (keeps the laptop cool)."""
    if "spark" in (config.getoption("-m") or ""):
        return
    skip = pytest.mark.skip(reason="Spark test: run with `pytest -m spark`")
    for item in items:
        if "spark" in item.keywords:
            item.add_marker(skip)
