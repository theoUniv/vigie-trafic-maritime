"""Collecteur météo : interroge Open-Meteo et publie les observations dans Kafka.

    python -m maritime.weather

Sortie : topic weather.marine (clé = « lat,lon » du point de grille)

C'est une deuxième source open data, collectée par un service indépendant : le producteur AIS
n'est pas modifié, et le processeur consomme simplement un topic de plus.
"""
from __future__ import annotations

import logging
import signal
import sys
import time

import requests

from .config import settings
from .kafka_io import JsonProducer, ensure_topics
from .sources.openmeteo import OpenMeteoSource, build_grid

log = logging.getLogger("weather")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    ensure_topics()
    producer = JsonProducer()
    source = OpenMeteoSource(build_grid(settings.weather_bbox, settings.weather_step),
                             cache_path="data/weather_points.json")
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    interval = settings.weather_interval_min * 60
    log.info("Open-Meteo : %d points (grille %s, pas %s), toutes les %d min",
             len(source.points), settings.weather_bbox, settings.weather_step,
             settings.weather_interval_min)

    while True:
        t0 = time.monotonic()
        try:
            obs = source.fetch()
            for o in obs:
                producer.send(settings.topics.weather, o, key=f"{o['lat']},{o['lon']}")
            producer.flush()
            log.info("Publié : %d observations météo marine", len(obs))
            wait = interval
        except (requests.RequestException, ValueError) as exc:
            log.warning("Open-Meteo indisponible (%s), nouvel essai dans 2 min", exc)
            wait = 120
        time.sleep(max(5, wait - (time.monotonic() - t0)))


if __name__ == "__main__":
    main()
