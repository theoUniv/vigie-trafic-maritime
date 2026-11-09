"""Producteur : collecte les positions AIS chaque minute et les publie dans Kafka.

    python -m maritime.producer
"""
from __future__ import annotations

import logging
import signal
import sys
import time

from .config import settings
from .kafka_io import JsonProducer, ensure_topics

log = logging.getLogger("producer")


def build_source():
    if settings.ais_source == "aisstream":
        from .sources.aisstream import AisStreamSource
        return AisStreamSource(settings.aisstream_api_key, settings.aisstream_bboxes,
                               settings.poll_interval_s)
    from .sources.digitraffic import DigitrafficSource
    return DigitrafficSource(settings.digitraffic_user, settings.poll_interval_s,
                             settings.static_refresh_s)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    ensure_topics()
    producer = JsonProducer()
    source = build_source()
    topics = settings.topics
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    log.info("Source AIS : %s — publication toutes les %d s vers %s",
             settings.ais_source, settings.poll_interval_s, settings.kafka_bootstrap)

    try:
        for batch in source.snapshots():
            ingested_at = int(time.time() * 1000)
            for msg in batch["static"]:
                msg["ingested_at"] = ingested_at
                producer.send(topics.static, msg, key=msg["mmsi"])
            for msg in batch["positions"]:
                msg["ingested_at"] = ingested_at
                # Clé = MMSI : toutes les positions d'un navire vont dans la même partition,
                # ce qui garantit leur ordre pour le traitement de trajectoire.
                producer.send(topics.positions, msg, key=msg["mmsi"])
            producer.flush()
            log.info("Publié : %d positions, %d fiches navires (erreurs cumulées : %d)",
                     len(batch["positions"]), len(batch["static"]), producer.errors)
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
