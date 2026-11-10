"""Transformations Spark : lecture des messages Kafka et agrégats par fenêtre de temps.

Les fonctions prennent et renvoient des DataFrames. Elles marchent aussi bien sur un flux
(`readStream`) que sur un DataFrame statique, ce qui permet de les tester sans Kafka.
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..geo import KM_PER_DEG_LAT

STOPPED_KN = 0.5   # mêmes seuils que le processeur
MOVING_KN = 1.0

# Format des messages publiés par le producteur sur ais.positions
POSITION_SCHEMA = T.StructType([
    T.StructField("mmsi", T.LongType()),
    T.StructField("ts", T.LongType()),            # epoch ms
    T.StructField("lat", T.DoubleType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("sog", T.DoubleType()),
    T.StructField("cog", T.DoubleType()),
    T.StructField("heading", T.DoubleType()),
    T.StructField("nav_status", T.IntegerType()),
    T.StructField("source", T.StringType()),
    T.StructField("ingested_at", T.LongType()),
])


def parse_positions(kafka_df: DataFrame) -> DataFrame:
    """Messages Kafka bruts (colonne `value` en JSON) -> positions typées avec `event_time`."""
    return (
        kafka_df
        .select(F.from_json(F.col("value").cast("string"), POSITION_SCHEMA).alias("p"))
        .select("p.*")
        .where(F.col("mmsi").isNotNull() & F.col("ts").isNotNull()
               & F.col("lat").between(-90, 90) & F.col("lon").between(-180, 180))
        # Temps de l'événement = horodatage AIS (pas l'heure de réception)
        .withColumn("event_time", (F.col("ts") / 1000).cast("timestamp"))
    )


def deduplicate(positions: DataFrame) -> DataFrame:
    """Supprime les positions reçues plusieurs fois (même navire, même horodatage AIS).

    Kafka garantit une livraison « au moins une fois » : au redémarrage, le producteur republie
    l'instantané Digitraffic, donc des positions déjà envoyées. Le processeur les ignore grâce à
    la clé (mmsi, ts) ; ici, sans dédoublonnage, elles seraient comptées deux fois. La colonne
    `event_time` fait partie de la clé : avec le filigrane, Spark oublie les clés trop anciennes et
    l'état reste borné.
    """
    return positions.dropDuplicates(["mmsi", "event_time"])


def zone_columns(lat: Column, lon: Column, cell_km: float) -> tuple[Column, Column]:
    """Indices (ligne, colonne) de la grille, en expressions Spark natives.

    Même calcul que `maritime.geo.cell_of` : la largeur en longitude d'une ligne dépend de la
    latitude de son centre pour que les cellules restent à peu près carrées.
    """
    dlat = cell_km / KM_PER_DEG_LAT
    i = F.floor((lat + 90) / F.lit(dlat))
    lat_center = (i + 0.5) * F.lit(dlat) - 90
    dlon = F.lit(dlat) / F.greatest(F.cos(F.radians(lat_center)), F.lit(0.05))
    j = F.floor((lon + 180) / dlon)
    return i, j


def with_category(positions: DataFrame, vessels: DataFrame) -> DataFrame:
    """Ajoute le type de navire (table `vessels`, alimentée par les messages AIS statiques)."""
    return (positions.join(F.broadcast(vessels.select("mmsi", "category")), "mmsi", "left")
            .withColumn("category", F.coalesce(F.col("category"), F.lit("other"))))


def port_lookup_udf(ports, radius_nm: float):
    """UDF : LOCODE du port le plus proche dans un rayon donné (index spatial du gazetteer)."""
    def lookup(lat, lon):
        if lat is None or lon is None:
            return None
        hit = ports.nearest(lat, lon, radius_nm)
        return hit[0].locode if hit else None
    return F.udf(lookup, T.StringType())


def _window_columns() -> list[Column]:
    return [F.col("window.start").cast("long").alias("window_start"),
            F.col("window.end").cast("long").alias("window_end")]


DISTINCT_RSD = 0.01   # erreur relative cible de HyperLogLog++ (5 % par défaut dans Spark)


def _distinct_vessels(condition: Column | None = None) -> Column:
    # approx_count_distinct (HyperLogLog++) : countDistinct n'est pas supporté en streaming.
    # Avec rsd = 1 %, l'état par groupe passe d'environ 0,4 à 13 Ko, ce qui reste négligeable ici.
    col = F.col("mmsi") if condition is None else F.when(condition, F.col("mmsi"))
    return F.approx_count_distinct(col, rsd=DISTINCT_RSD)


def zone_traffic(positions: DataFrame, window: str, cell_km: float) -> DataFrame:
    """Trafic par fenêtre, zone de la grille et type de navire."""
    i, j = zone_columns(F.col("lat"), F.col("lon"), cell_km)
    return (
        positions.withColumn("zone_i", i).withColumn("zone_j", j)
        .groupBy(F.window("event_time", window).alias("window"), "zone_i", "zone_j", "category")
        .agg(_distinct_vessels().alias("n_vessels"),
             F.count(F.lit(1)).alias("n_positions"),
             _distinct_vessels(F.col("sog") < STOPPED_KN).alias("n_stopped"),
             F.avg("sog").alias("mean_sog"),
             F.max("sog").alias("max_sog"))
        .select(*_window_columns(), "zone_i", "zone_j", "category", "n_vessels",
                "n_positions", "n_stopped", "mean_sog", "max_sog")
    )


def category_traffic(positions: DataFrame, window: str) -> DataFrame:
    """Trafic par fenêtre et type de navire, sur toute la zone couverte."""
    moving = F.col("sog") >= MOVING_KN
    return (
        positions
        .groupBy(F.window("event_time", window).alias("window"), "category")
        .agg(_distinct_vessels().alias("n_vessels"),
             _distinct_vessels(moving).alias("n_moving"),
             F.count(F.lit(1)).alias("n_positions"),
             F.avg("sog").alias("mean_sog"),
             F.avg(F.when(moving, F.col("sog"))).alias("mean_sog_moving"))
        .select(*_window_columns(), "category", "n_vessels", "n_moving", "n_positions",
                "mean_sog", "mean_sog_moving")
    )


def port_traffic(positions: DataFrame, window: str, ports, radius_nm: float) -> DataFrame:
    """Fréquentation des ports : navires présents dans le rayon de chaque port, par fenêtre."""
    lookup = port_lookup_udf(ports, radius_nm)
    return (
        positions.withColumn("locode", lookup(F.col("lat"), F.col("lon")))
        .where(F.col("locode").isNotNull())
        .groupBy(F.window("event_time", window).alias("window"), "locode")
        .agg(_distinct_vessels().alias("n_vessels"),
             _distinct_vessels(F.col("sog") >= MOVING_KN).alias("n_moving"),
             _distinct_vessels(F.col("sog") < STOPPED_KN).alias("n_stopped"),
             F.count(F.lit(1)).alias("n_positions"),
             F.avg("sog").alias("mean_sog"))
        .select(*_window_columns(), "locode", "n_vessels", "n_moving", "n_stopped",
                "n_positions", "mean_sog")
    )
