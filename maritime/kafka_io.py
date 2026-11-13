"""Accès Apache Kafka : création des topics, producteur et consommateur JSON."""
from __future__ import annotations

import json
import logging
import time

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from .config import settings

log = logging.getLogger(__name__)

# topic -> (partitions, rétention ms)
TOPIC_SPECS = {
    settings.topics.positions: (3, 24 * 3600 * 1000),
    settings.topics.static: (1, 7 * 24 * 3600 * 1000),
    settings.topics.alerts: (1, 7 * 24 * 3600 * 1000),
    settings.topics.congestion: (1, 24 * 3600 * 1000),
    settings.topics.briefings: (1, 7 * 24 * 3600 * 1000),
    settings.topics.weather: (1, 7 * 24 * 3600 * 1000),
}


def wait_for_kafka(bootstrap: str, timeout_s: int = 120) -> AdminClient:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.time() + timeout_s
    while True:
        try:
            admin.list_topics(timeout=5)
            return admin
        except KafkaException as exc:
            if time.time() > deadline:
                raise
            log.info("Kafka pas encore prêt (%s), nouvelle tentative...", exc)
            time.sleep(3)


def ensure_topics(bootstrap: str | None = None) -> None:
    bootstrap = bootstrap or settings.kafka_bootstrap
    admin = wait_for_kafka(bootstrap)
    existing = set(admin.list_topics(timeout=10).topics)
    new = [
        NewTopic(name, num_partitions=parts, replication_factor=1,
                 config={"retention.ms": str(retention)})
        for name, (parts, retention) in TOPIC_SPECS.items() if name not in existing
    ]
    if not new:
        return
    for name, fut in admin.create_topics(new).items():
        try:
            fut.result()
            log.info("Topic créé : %s", name)
        except KafkaException as exc:
            log.warning("Topic %s : %s", name, exc)


class JsonProducer:
    def __init__(self, bootstrap: str | None = None):
        self._p = Producer({
            "bootstrap.servers": bootstrap or settings.kafka_bootstrap,
            "linger.ms": 50,
            "compression.type": "lz4",
            "enable.idempotence": True,
        })
        self.errors = 0

    def _on_delivery(self, err, _msg):
        if err is not None:
            self.errors += 1
            log.error("Échec de livraison Kafka : %s", err)

    def send(self, topic: str, value: dict, key: str | int | None = None) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode()
        while True:
            try:
                self._p.produce(topic, value=payload,
                                key=str(key).encode() if key is not None else None,
                                on_delivery=self._on_delivery)
                break
            except BufferError:  # file locale pleine : on laisse partir les messages
                self._p.poll(0.5)
        self._p.poll(0)

    def flush(self, timeout: float = 30) -> int:
        return self._p.flush(timeout)


def json_consumer(group_id: str, topics: list[str], bootstrap: str | None = None,
                  offset_reset: str = "earliest") -> Consumer:
    c = Consumer({
        "bootstrap.servers": bootstrap or settings.kafka_bootstrap,
        "group.id": group_id,
        "auto.offset.reset": offset_reset,
        "enable.auto.commit": True,
    })
    c.subscribe(topics)
    return c


def decode(msg) -> dict | None:
    try:
        return json.loads(msg.value())
    except (TypeError, ValueError):
        log.warning("Message illisible ignoré sur %s", msg.topic())
        return None
