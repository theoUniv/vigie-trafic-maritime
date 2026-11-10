"""Rédacteur de bulletins : consomme les alertes et zones Kafka, fait rédiger un bulletin par le LLM.

    python -m maritime.briefer

Entrées  : ais.alerts, ais.congestion (Kafka) ; indicateurs de flotte (PostgreSQL)
Sorties  : topic ais.briefings et table briefings

Nouveau consommateur ajouté sans toucher au producteur ni au processeur : c'est l'intérêt du
découplage par Kafka. Si le LLM est indisponible, un bulletin gabarit est produit à la place.
"""
from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time

from psycopg.rows import dict_row

from .config import settings
from .db import connect, init_schema
from .kafka_io import JsonProducer, decode, ensure_topics, json_consumer
from .llm.briefing import briefing_messages, build_facts, clean_output, template_bulletin
from .llm.client import LLMError, OllamaClient

log = logging.getLogger("briefer")

MAP_WINDOW_MIN = int(os.environ.get("MAP_WINDOW_MIN", 30))
FIRST_DELAY_S = 60   # laisse le temps de rattraper les messages Kafka au démarrage


def fleet_kpi(conn) -> dict:
    with conn.cursor(row_factory=dict_row) as cur:
        return cur.execute(f"""
            SELECT count(*) AS active,
                   count(*) FILTER (WHERE sog >= 1) AS moving,
                   count(*) FILTER (WHERE nav_status = 1) AS anchored,
                   count(*) FILTER (WHERE nav_status = 5) AS moored,
                   count(*) FILTER (WHERE eta_estimate->>'status'
                                    IN ('retard_probable','retard_important')) AS delayed,
                   round(avg(sog) FILTER (WHERE sog >= 1)::numeric, 1) AS mean_speed
            FROM vessel_state WHERE ts > now() - interval '{MAP_WINDOW_MIN} minutes'""").fetchone()


class Briefer:
    def __init__(self, conn, producer: JsonProducer | None, client: OllamaClient):
        self.conn = conn
        self.producer = producer
        self.client = client
        self.alerts: list[dict] = []
        self.cells: list[dict] = []

    def on_message(self, topic: str, value: dict) -> None:
        if topic == settings.topics.alerts:
            self.alerts.append(value)
        elif topic == settings.topics.congestion:
            self.cells = value.get("cells", [])

    def prune(self, now_ms: int) -> None:
        since = now_ms - settings.briefing_window_min * 60000
        self.alerts = [a for a in self.alerts if (a.get("ts") or 0) >= since]

    def generate(self, facts: dict) -> tuple[str, str, float]:
        t0 = time.time()
        if self.client.enabled:
            try:
                msg = self.client.chat(briefing_messages(facts), temperature=0.3)
                text = clean_output(msg.get("content", ""))
                if text:
                    return text, "llm", time.time() - t0
                log.warning("Réponse vide du LLM, repli sur le gabarit")
            except LLMError as exc:
                log.warning("LLM indisponible (%s), repli sur le gabarit", exc)
        return template_bulletin(facts), "template", time.time() - t0

    def run_once(self) -> dict:
        now_ms = int(time.time() * 1000)
        self.prune(now_ms)
        facts = build_facts(self.alerts, self.cells, fleet_kpi(self.conn),
                            settings.briefing_window_min, now_ms)
        text, generator, duration = self.generate(facts)
        model = self.client.model if generator == "llm" else None
        self.conn.execute(
            """INSERT INTO briefings (window_min, text, generator, model, duration_s, facts)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (settings.briefing_window_min, text, generator, model, duration,
             json.dumps(facts, ensure_ascii=False)))
        payload = {"ts": now_ms, "window_min": settings.briefing_window_min, "text": text,
                   "generator": generator, "model": model, "facts": facts}
        if self.producer:
            self.producer.send(settings.topics.briefings, payload)
            self.producer.flush(5)
        log.info("Bulletin publié (%s, %.1f s, %d alertes, %d zones)", generator, duration,
                 facts["alertes_total"], len(facts["zones"]))
        return payload


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    ensure_topics()
    conn = connect(settings.database_url)
    init_schema(conn)
    client = OllamaClient(settings.llm_url, settings.llm_model, settings.llm_timeout_s)
    briefer = Briefer(conn, JsonProducer(), client)
    consumer = json_consumer("vigie-briefer", [settings.topics.alerts, settings.topics.congestion])
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    log.info("Bulletin toutes les %d min sur %d min glissantes — LLM : %s",
             settings.briefing_interval_min, settings.briefing_window_min,
             f"{client.model} @ {client.url}" if client.enabled else "désactivé")

    next_at = time.time() + FIRST_DELAY_S
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                value = decode(msg)
                if value is not None:
                    briefer.on_message(msg.topic(), value)
            if time.time() >= next_at:
                try:
                    briefer.run_once()
                except Exception:   # noqa: BLE001 — on ne tue pas le service pour un bulletin
                    log.exception("Échec de génération du bulletin")
                next_at = time.time() + settings.briefing_interval_min * 60
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
