"""Configuration du job Spark (variables d'environnement préfixées SPARK_)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from urllib.parse import unquote, urlsplit

POSTGRES_JDBC_PACKAGE = "org.postgresql:postgresql:42.7.4"


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class SparkJobSettings:
    master: str = field(default_factory=lambda: _env("SPARK_MASTER", "local[2]"))
    driver_memory: str = field(default_factory=lambda: _env("SPARK_DRIVER_MEMORY", "1g"))
    # Agrégation : fenêtres fixes de 5 min, données en retard acceptées pendant 10 min
    window: str = field(default_factory=lambda: _env("SPARK_WINDOW", "5 minutes"))
    watermark: str = field(default_factory=lambda: _env("SPARK_WATERMARK", "10 minutes"))
    trigger: str = field(default_factory=lambda: _env("SPARK_TRIGGER", "1 minute"))
    # Zones : grille plus large que celle du processeur (5 km), adaptée à une vue d'ensemble
    zone_km: float = field(default_factory=lambda: float(_env("SPARK_ZONE_KM", "20")))
    port_radius_nm: float = field(default_factory=lambda: float(_env("SPARK_PORT_RADIUS_NM", "5")))
    # Kafka : au premier démarrage on relit l'historique du topic (24 h), par lots bornés
    starting_offsets: str = field(default_factory=lambda: _env("SPARK_STARTING_OFFSETS", "earliest"))
    max_offsets_per_trigger: int = field(
        default_factory=lambda: int(_env("SPARK_MAX_OFFSETS_PER_TRIGGER", "200000")))
    checkpoint_dir: str = field(default_factory=lambda: _env("SPARK_CHECKPOINT_DIR", "data/spark-checkpoints"))
    ivy_dir: str = field(default_factory=lambda: _env("SPARK_IVY_DIR", ""))
    retention_hours: int = field(default_factory=lambda: int(_env("SPARK_RETENTION_HOURS", "168")))


def jdbc_options(database_url: str) -> dict[str, str]:
    """Convertit une URL `postgresql://user:mdp@hôte:port/base` en options JDBC Spark."""
    u = urlsplit(database_url)
    if u.scheme not in ("postgresql", "postgres"):
        raise ValueError(f"URL PostgreSQL attendue, reçu : {u.scheme}://")
    host = u.hostname or "localhost"
    port = u.port or 5432
    return {
        "url": f"jdbc:postgresql://{host}:{port}{u.path or '/'}",
        "user": unquote(u.username or ""),
        "password": unquote(u.password or ""),
        "driver": "org.postgresql.Driver",
    }
