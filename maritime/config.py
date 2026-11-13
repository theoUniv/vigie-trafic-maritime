"""Configuration centralisée, lue depuis les variables d'environnement."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _env_float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


@dataclass(frozen=True)
class Topics:
    positions: str = "ais.positions"
    static: str = "ais.static"
    alerts: str = "ais.alerts"
    congestion: str = "ais.congestion"
    briefings: str = "ais.briefings"
    weather: str = "weather.marine"


@dataclass(frozen=True)
class Settings:
    # Kafka
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9092"))
    topics: Topics = field(default_factory=Topics)

    # Source AIS : "digitraffic" (open data, sans clé) ou "aisstream" (mondial, clé gratuite)
    ais_source: str = field(default_factory=lambda: _env("AIS_SOURCE", "digitraffic"))
    poll_interval_s: int = field(default_factory=lambda: _env_int("POLL_INTERVAL_S", 60))
    static_refresh_s: int = field(default_factory=lambda: _env_int("STATIC_REFRESH_S", 600))
    digitraffic_user: str = field(default_factory=lambda: _env("DIGITRAFFIC_USER", "ipssi-open-data/vigie"))
    aisstream_api_key: str = field(default_factory=lambda: _env("AISSTREAM_API_KEY", ""))
    # Boîtes englobantes aisstream : "lat1,lon1,lat2,lon2;..." (défaut : monde entier)
    aisstream_bboxes: str = field(default_factory=lambda: _env("AISSTREAM_BBOXES", "-90,-180,90,180"))

    # Base de données
    database_url: str = field(
        default_factory=lambda: _env("DATABASE_URL", "postgresql://vigie:vigie@localhost:5433/vigie")
    )
    retention_hours: int = field(default_factory=lambda: _env_int("RETENTION_HOURS", 48))

    # Traitement
    tick_interval_s: int = field(default_factory=lambda: _env_int("TICK_INTERVAL_S", 60))
    active_window_min: int = field(default_factory=lambda: _env_int("ACTIVE_WINDOW_MIN", 15))
    cell_km: float = field(default_factory=lambda: _env_float("CELL_KM", 5.0))
    congestion_min_vessels: int = field(default_factory=lambda: _env_int("CONGESTION_MIN_VESSELS", 3))
    ais_gap_min: int = field(default_factory=lambda: _env_int("AIS_GAP_MIN", 20))
    alert_cooldown_min: int = field(default_factory=lambda: _env_int("ALERT_COOLDOWN_MIN", 30))
    ports_cache: str = field(default_factory=lambda: _env("PORTS_CACHE", "data/ports.json"))

    # Météo marine Open-Meteo (open data, sans clé) : grille "sud,ouest,nord,est" au pas "dlat,dlon"
    weather_bbox: str = field(default_factory=lambda: _env("WEATHER_BBOX", "53.5,9.5,66,30.5"))
    weather_step: str = field(default_factory=lambda: _env("WEATHER_STEP", "0.5,1"))
    weather_interval_min: int = field(default_factory=lambda: _env_int("WEATHER_INTERVAL_MIN", 90))

    # LLM open source servi localement par Ollama (vide = fonctionnalités IA désactivées)
    llm_url: str = field(default_factory=lambda: _env("LLM_URL", "http://localhost:11434"))
    llm_model: str = field(default_factory=lambda: _env("LLM_MODEL", "qwen2.5:3b"))
    llm_timeout_s: int = field(default_factory=lambda: _env_int("LLM_TIMEOUT_S", 180))
    briefing_interval_min: int = field(default_factory=lambda: _env_int("BRIEFING_INTERVAL_MIN", 15))
    briefing_window_min: int = field(default_factory=lambda: _env_int("BRIEFING_WINDOW_MIN", 60))


settings = Settings()
