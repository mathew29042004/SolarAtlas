"""Delta housekeeping: compact small files, Z-order buildings by location, drop old versions."""
from delta.tables import DeltaTable

from solaratlas import config
from solaratlas.lakehouse.spark import get, stop

RETAIN_HOURS = 168  # keep 7 days of time travel
# Z-order the tables the API and bbox queries filter by location, so Delta can skip files.
ZORDER = {("bronze", "buildings"): ["xmin", "ymin"], ("silver", "buildings"): ["lon", "lat"],
          ("gold", "building_solar"): ["lon", "lat"]}


def main():
    spark = get()
    for layer_dir in sorted(config.LAKE.iterdir()):
        for t in sorted(p for p in layer_dir.iterdir() if (p / "_delta_log").exists()):
            dt = DeltaTable.forPath(spark, str(t))
            cols = ZORDER.get((layer_dir.name, t.name))
            if cols:
                dt.optimize().executeZOrderBy(*cols)
            else:
                dt.optimize().executeCompaction()
            dt.vacuum(RETAIN_HOURS)
            print(f"maintained {layer_dir.name}.{t.name}")
    stop()


if __name__ == "__main__":
    main()
