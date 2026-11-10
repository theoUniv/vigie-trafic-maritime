"""Job Spark Structured Streaming : Kafka (ais.positions) -> agrégats par fenêtre -> PostgreSQL.

    python -m maritime.spark.job                 # lance les trois requêtes de streaming
    python -m maritime.spark.job --resolve-only  # télécharge les connecteurs (build Docker)

Trois requêtes partagent la même source Kafka :
  * zones      -> spark_zone_traffic      (zone de la grille x type de navire)
  * categories -> spark_category_traffic  (type de navire)
  * ports      -> spark_port_traffic      (fréquentation des ports)
Chaque requête a son propre checkpoint : après un redémarrage, Spark reprend aux offsets
Kafka et à l'état d'agrégation enregistrés.
"""
from __future__ import annotations

import logging
import sys
import time

from ..config import settings
from ..db import connect, init_schema
from ..ports import load_ports
from .config import POSTGRES_JDBC_PACKAGE, SparkJobSettings, jdbc_options
from .sink import SparkSink, category_rows, port_rows, zone_rows

log = logging.getLogger("spark-job")


def build_spark(cfg: SparkJobSettings):
    import pyspark
    from pyspark.sql import SparkSession

    # Le connecteur Kafka doit avoir la même version que Spark (Scala 2.12 pour PySpark 3.5)
    packages = [f"org.apache.spark:spark-sql-kafka-0-10_2.12:{pyspark.__version__}", POSTGRES_JDBC_PACKAGE]
    builder = (SparkSession.builder.appName("vigie-streaming").master(cfg.master)
               .config("spark.jars.packages", ",".join(packages))
               .config("spark.driver.memory", cfg.driver_memory)
               .config("spark.sql.session.timeZone", "UTC")
               # Peu de données par micro-lot : 4 partitions au lieu de 200 (état plus léger)
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.streaming.metricsEnabled", "true")
               .config("spark.ui.showConsoleProgress", "false"))
    if cfg.ivy_dir:
        builder = builder.config("spark.jars.ivy", cfg.ivy_dir)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # Avertissement sans conséquence répété à chaque lecture Kafka depuis foreachBatch
    jvm = spark.sparkContext._jvm
    jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(
        "org.apache.spark.sql.kafka010.consumer.KafkaDataConsumer", jvm.org.apache.logging.log4j.Level.ERROR)
    return spark


def wait_for_vessels(conn, timeout_s: int = 300) -> None:
    """Attend que le processeur ait rempli la table `vessels`. Sinon, au premier démarrage, la
    jointure ne trouve aucun type et les fenêtres relues depuis Kafka seraient toutes « other »."""
    deadline = time.monotonic() + timeout_s
    while conn.execute("SELECT count(*) FROM vessels WHERE ship_type IS NOT NULL").fetchone()[0] == 0:
        if time.monotonic() > deadline:
            log.warning("Table vessels toujours vide : démarrage sans type de navire")
            return
        log.info("En attente des fiches navires (table vessels)...")
        time.sleep(10)


def read_positions(spark, cfg: SparkJobSettings):
    from .transforms import deduplicate, parse_positions

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", settings.kafka_bootstrap)
           .option("subscribe", settings.topics.positions)
           .option("startingOffsets", cfg.starting_offsets)
           .option("maxOffsetsPerTrigger", cfg.max_offsets_per_trigger)
           # Les messages expirés (rétention 24 h) pendant un arrêt ne bloquent pas la reprise
           .option("failOnDataLoss", "false")
           .load())
    # Filigrane : une fenêtre est finalisée (et son état libéré) 10 min après sa fin.
    # Il borne aussi l'état du dédoublonnage.
    return deduplicate(parse_positions(raw).withWatermark("event_time", cfg.watermark))


def read_vessels(spark):
    """Table `vessels` (type de navire) lue en JDBC. Jointure flux-statique : Spark la relit à
    chaque micro-lot, les nouveaux navires sont donc pris en compte sans redémarrage."""
    return (spark.read.format("jdbc").options(**jdbc_options(settings.database_url))
            .option("dbtable", "(SELECT mmsi, category FROM vessels) AS v").load())


def start_query(df, name: str, write, cfg: SparkJobSettings):
    def batch(batch_df, batch_id: int) -> None:
        t0 = time.monotonic()
        rows = [r.asDict() for r in batch_df.collect()]   # agrégats : quelques milliers de lignes au plus
        n = write(rows)
        log.info("[%s] lot %d : %d ligne(s) écrite(s) en %.1f s", name, batch_id, n, time.monotonic() - t0)

    return (df.writeStream.queryName(name)
            .outputMode("update")          # seules les fenêtres modifiées sont réécrites (upsert)
            .foreachBatch(batch)
            .option("checkpointLocation", f"{cfg.checkpoint_dir.rstrip('/')}/{name}")
            .trigger(processingTime=cfg.trigger)
            .start())


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    argv = sys.argv[1:] if argv is None else argv
    cfg = SparkJobSettings()
    spark = build_spark(cfg)
    if "--resolve-only" in argv:
        log.info("Connecteurs Spark téléchargés")
        spark.stop()
        return

    from .transforms import category_traffic, port_traffic, with_category, zone_traffic

    conn = connect(settings.database_url)
    init_schema(conn)          # garantit l'existence de la table vessels (jointure)
    sink = SparkSink(conn, cfg.retention_hours)
    sink.init_schema()
    wait_for_vessels(conn)
    ports =load_ports(settings.ports_cache, settings.digitraffic_user)

    positions = read_positions(spark, cfg)
    typed = with_category(positions, read_vessels(spark))

    queries = [
        start_query(zone_traffic(typed, cfg.window, cfg.zone_km), "zones",
                    lambda rows: sink.write("spark_zone_traffic", zone_rows(rows, cfg.zone_km)), cfg),
        start_query(category_traffic(typed, cfg.window), "categories",
                    lambda rows: sink.write("spark_category_traffic", category_rows(rows)), cfg),
        start_query(port_traffic(positions, cfg.window, ports, cfg.port_radius_nm), "ports",
                    lambda rows: sink.write("spark_port_traffic", port_rows(rows, ports)), cfg),
    ]
    log.info("Streaming démarré : %s ; fenêtres de %s, filigrane %s, déclenchement toutes les %s, "
             "zones de %.0f km, Spark UI sur le port 4040",
             ", ".join(q.name for q in queries), cfg.window, cfg.watermark, cfg.trigger, cfg.zone_km)
    try:
        spark.streams.awaitAnyTermination()
    finally:
        for q in queries:
            q.stop()
        spark.stop()


if __name__ == "__main__":
    main()
