"""Tests du job Spark.

La conversion des lignes et la configuration se testent sans Spark. Les agrégats sont testés sur
des DataFrames statiques (mêmes fonctions que le flux) : ces tests demandent pyspark et Java,
et sont ignorés sinon. Pour les lancer avec l'image Spark :
    docker compose run --rm -v ./tests:/app/tests spark python -m pytest -q tests/test_spark.py
"""
import json
import shutil
from datetime import datetime, timezone

import pytest

from maritime.geo import cell_bounds, cell_of
from maritime.ports import Port, PortIndex
from maritime.spark.config import jdbc_options
from maritime.spark.sink import category_rows, port_rows, zone_rows

HELSINKI = Port("FIHEL", "Helsinki", "Finland", 60.1567, 24.9550)
TALLINN = Port("EETLL", "Tallinn", "Estonia", 59.4450, 24.7650)
PORTS = PortIndex([HELSINKI, TALLINN])
# 10:00:00 UTC pile : début d'une fenêtre de 5 min
T0 = int(datetime(2026, 11, 10, 10, 0, tzinfo=timezone.utc).timestamp() * 1000)
MIN = 60_000


# ------------------------------------------------------------------ sans Spark
def test_jdbc_options_from_database_url():
    o = jdbc_options("postgresql://vigie:p%40ss@postgres:5432/vigie")
    assert o["url"] == "jdbc:postgresql://postgres:5432/vigie"
    assert (o["user"], o["password"], o["driver"]) == ("vigie", "p@ss", "org.postgresql.Driver")
    assert jdbc_options("postgresql://u@localhost/db")["url"] == "jdbc:postgresql://localhost:5432/db"
    with pytest.raises(ValueError):
        jdbc_options("mysql://u@h/db")


def test_zone_rows_add_id_and_bounds():
    row = {"window_start": 100, "window_end": 400, "zone_i": 12, "zone_j": -3, "category": "cargo",
           "n_vessels": 4, "n_positions": 20, "n_stopped": 1, "mean_sog": 9.876, "max_sog": None}
    (out,) = zone_rows([row], 20)
    assert out["zone_id"] == "12:-3"
    assert (out["lat_min"], out["lon_min"], out["lat_max"], out["lon_max"]) == cell_bounds(12, -3, 20)
    assert out["mean_sog"] == 9.88 and out["max_sog"] is None


def test_port_and_category_rows():
    (p,) = port_rows([{"locode": "FIHEL", "mean_sog": 1.234}], PORTS)
    assert (p["port_name"], p["lat"], p["mean_sog"]) == ("Helsinki", HELSINKI.lat, 1.23)
    (u,) = port_rows([{"locode": "XXXXX", "mean_sog": None}], PORTS)
    assert u["port_name"] is None and u["mean_sog"] is None
    (c,) = category_rows([{"category": "cargo", "mean_sog": 3.333, "mean_sog_moving": None}])
    assert c["mean_sog"] == 3.33 and c["mean_sog_moving"] is None


# ------------------------------------------------------------------ avec Spark
try:
    import pyspark  # noqa: F401
    HAS_SPARK = shutil.which("java") is not None
except ImportError:
    HAS_SPARK = False
needs_spark = pytest.mark.skipif(not HAS_SPARK, reason="pyspark ou Java indisponible")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[1]").appName("vigie-tests")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def _kafka_df(spark, messages):
    """Simule la sortie de la source Kafka : une colonne binaire `value` contenant le JSON."""
    return spark.createDataFrame([(json.dumps(m).encode(),) for m in messages], "value binary")


def _pos(mmsi, minute, lat, lon, sog):
    return {"mmsi": mmsi, "ts": T0 + minute * MIN, "lat": lat, "lon": lon, "sog": sog, "cog": 90.0,
            "heading": 90, "nav_status": 0, "source": "test", "ingested_at": T0}


@needs_spark
def test_parse_positions_filters_invalid(spark):
    from maritime.spark.transforms import parse_positions
    df = parse_positions(_kafka_df(spark, [
        _pos(1, 0, 60.0, 25.0, 10.0),
        _pos(2, 0, 95.0, 25.0, 10.0),      # latitude impossible
        {"mmsi": 3, "lat": 60.0},           # sans horodatage
    ]))
    rows = df.collect()
    assert [r.mmsi for r in rows] == [1]
    assert rows[0].event_time.replace(tzinfo=timezone.utc).timestamp() * 1000 == T0


@needs_spark
def test_duplicate_positions_counted_once(spark):
    """Le producteur republie l'instantané à son redémarrage : un doublon ne compte qu'une fois."""
    from maritime.spark.transforms import category_traffic, deduplicate, parse_positions, with_category
    msgs = [_pos(1, 0, 60.1, 24.9, 10.0), _pos(1, 1, 60.11, 24.91, 10.0),
            _pos(1, 1, 60.11, 24.91, 10.0),   # même navire, même horodatage : doublon
            _pos(2, 1, 60.11, 24.91, 4.0)]
    positions = deduplicate(parse_positions(_kafka_df(spark, msgs)))
    assert positions.count() == 3
    vessels = spark.createDataFrame([(1, "cargo"), (2, "cargo")], "mmsi long, category string")
    (row,) = category_traffic(with_category(positions, vessels), "5 minutes").collect()
    assert (row.n_vessels, row.n_positions) == (2, 3)
    assert row.mean_sog == pytest.approx(8.0)


@needs_spark
@pytest.mark.parametrize("lat,lon", [(60.1567, 24.955), (59.445, 24.765), (-33.9, 18.4), (0.0, -0.01),
                                     (78.2, 15.6), (-89.9, 179.9)])
def test_spark_zone_matches_python_grid(spark, lat, lon):
    from pyspark.sql import functions as F
    from maritime.spark.transforms import zone_columns
    i, j = zone_columns(F.col("lat"), F.col("lon"), 20)
    row = spark.createDataFrame([(lat, lon)], "lat double, lon double").select(i.alias("i"), j.alias("j")).first()
    assert (row.i, row.j) == cell_of(lat, lon, 20)


@needs_spark
def test_zone_and_category_traffic_by_window(spark):
    from maritime.spark.transforms import category_traffic, parse_positions, with_category, zone_traffic
    msgs = [
        _pos(1, 0, 60.10, 24.90, 12.0), _pos(1, 2, 60.11, 24.92, 14.0),   # cargo en route
        _pos(2, 1, 60.10, 24.91, 0.1),                                     # cargo à l'arrêt, même zone
        _pos(3, 3, 60.10, 24.90, 20.0),                                    # passagers
        _pos(4, 4, 60.10, 24.90, 5.0),                                     # type inconnu -> other
        _pos(1, 6, 60.12, 24.95, 13.0),                                    # fenêtre suivante
    ]
    vessels = spark.createDataFrame([(1, "cargo"), (2, "cargo"), (3, "passenger")],
                                    "mmsi long, category string")
    typed = with_category(parse_positions(_kafka_df(spark, msgs)), vessels)

    zones = {(r.window_start, r.category): r for r in zone_traffic(typed, "5 minutes", 20).collect()}
    w0, w1 = T0 // 1000, T0 // 1000 + 300
    cargo = zones[(w0, "cargo")]
    assert (cargo.n_vessels, cargo.n_positions, cargo.n_stopped) == (2, 3, 1)
    assert cargo.mean_sog == pytest.approx((12 + 14 + 0.1) / 3)
    assert cargo.max_sog == 14.0 and cargo.window_end == w0 + 300
    assert (cargo.zone_i, cargo.zone_j) == cell_of(60.10, 24.90, 20)
    assert zones[(w0, "other")].n_vessels == 1
    assert zones[(w1, "cargo")].n_positions == 1

    cats = {(r.window_start, r.category): r for r in category_traffic(typed, "5 minutes").collect()}
    c = cats[(w0, "cargo")]
    assert (c.n_vessels, c.n_moving, c.n_positions) == (2, 1, 3)
    assert c.mean_sog_moving == pytest.approx(13.0)
    assert cats[(w0, "passenger")].n_moving == 1


@needs_spark
def test_port_traffic_counts_vessels_near_each_port(spark):
    from maritime.spark.transforms import parse_positions, port_traffic
    msgs = [
        _pos(10, 0, 60.155, 24.950, 0.2),   # Helsinki, à quai
        _pos(11, 1, 60.150, 24.960, 6.0),   # Helsinki, en mouvement
        _pos(11, 2, 60.152, 24.958, 5.0),
        _pos(12, 0, 59.450, 24.760, 0.0),   # Tallinn
        _pos(13, 0, 59.800, 24.800, 15.0),  # en mer, loin des ports
    ]
    rows = {r.locode: r for r in port_traffic(parse_positions(_kafka_df(spark, msgs)), "5 minutes",
                                              PORTS, 5).collect()}
    assert set(rows) == {"FIHEL", "EETLL"}
    hel = rows["FIHEL"]
    assert (hel.n_vessels, hel.n_moving, hel.n_stopped, hel.n_positions) == (2, 1, 1, 3)
    assert rows["EETLL"].n_stopped == 1
