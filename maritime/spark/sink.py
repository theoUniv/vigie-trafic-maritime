"""Écriture des agrégats Spark dans PostgreSQL (sans dépendance à pyspark).

En mode `update`, Spark renvoie à chaque micro-lot les fenêtres dont l'agrégat a changé :
on les écrit par upsert sur (début de fenêtre, clé), la dernière valeur remplaçant la précédente.
"""
from __future__ import annotations

import logging
import threading
import time

from ..geo import cell_bounds

log = logging.getLogger(__name__)

SCHEMA = """
-- Trafic par zone (grille de SPARK_ZONE_KM) et par type de navire, par fenêtre de 5 min
CREATE TABLE IF NOT EXISTS spark_zone_traffic (
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    zone_id       TEXT NOT NULL,
    category      TEXT NOT NULL,
    lat_min DOUBLE PRECISION, lon_min DOUBLE PRECISION,
    lat_max DOUBLE PRECISION, lon_max DOUBLE PRECISION,
    n_vessels     INT,
    n_positions   INT,
    n_stopped     INT,
    mean_sog      REAL,
    max_sog       REAL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, zone_id, category)
);

-- Trafic par type de navire sur toute la zone couverte
CREATE TABLE IF NOT EXISTS spark_category_traffic (
    window_start     TIMESTAMPTZ NOT NULL,
    window_end       TIMESTAMPTZ NOT NULL,
    category         TEXT NOT NULL,
    n_vessels        INT,
    n_moving         INT,
    n_positions      INT,
    mean_sog         REAL,
    mean_sog_moving  REAL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, category)
);

-- Fréquentation des ports : navires présents dans un rayon de SPARK_PORT_RADIUS_NM
CREATE TABLE IF NOT EXISTS spark_port_traffic (
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    locode        TEXT NOT NULL,
    port_name     TEXT,
    lat           DOUBLE PRECISION,
    lon           DOUBLE PRECISION,
    n_vessels     INT,
    n_moving      INT,
    n_stopped     INT,
    n_positions   INT,
    mean_sog      REAL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, locode)
);
CREATE INDEX IF NOT EXISTS spark_zone_traffic_ws_idx ON spark_zone_traffic (window_start DESC);
CREATE INDEX IF NOT EXISTS spark_port_traffic_ws_idx ON spark_port_traffic (window_start DESC);
"""

_UPSERT = {
    "spark_zone_traffic": """
        INSERT INTO spark_zone_traffic (window_start, window_end, zone_id, category, lat_min, lon_min,
            lat_max, lon_max, n_vessels, n_positions, n_stopped, mean_sog, max_sog, updated_at)
        VALUES (to_timestamp(%(window_start)s), to_timestamp(%(window_end)s), %(zone_id)s, %(category)s,
            %(lat_min)s, %(lon_min)s, %(lat_max)s, %(lon_max)s, %(n_vessels)s, %(n_positions)s,
            %(n_stopped)s, %(mean_sog)s, %(max_sog)s, now())
        ON CONFLICT (window_start, zone_id, category) DO UPDATE SET
            n_vessels = EXCLUDED.n_vessels, n_positions = EXCLUDED.n_positions,
            n_stopped = EXCLUDED.n_stopped, mean_sog = EXCLUDED.mean_sog,
            max_sog = EXCLUDED.max_sog, updated_at = now()""",
    "spark_category_traffic": """
        INSERT INTO spark_category_traffic (window_start, window_end, category, n_vessels, n_moving,
            n_positions, mean_sog, mean_sog_moving, updated_at)
        VALUES (to_timestamp(%(window_start)s), to_timestamp(%(window_end)s), %(category)s,
            %(n_vessels)s, %(n_moving)s, %(n_positions)s, %(mean_sog)s, %(mean_sog_moving)s, now())
        ON CONFLICT (window_start, category) DO UPDATE SET
            n_vessels = EXCLUDED.n_vessels, n_moving = EXCLUDED.n_moving,
            n_positions = EXCLUDED.n_positions, mean_sog = EXCLUDED.mean_sog,
            mean_sog_moving = EXCLUDED.mean_sog_moving, updated_at = now()""",
    "spark_port_traffic": """
        INSERT INTO spark_port_traffic (window_start, window_end, locode, port_name, lat, lon,
            n_vessels, n_moving, n_stopped, n_positions, mean_sog, updated_at)
        VALUES (to_timestamp(%(window_start)s), to_timestamp(%(window_end)s), %(locode)s, %(port_name)s,
            %(lat)s, %(lon)s, %(n_vessels)s, %(n_moving)s, %(n_stopped)s, %(n_positions)s,
            %(mean_sog)s, now())
        ON CONFLICT (window_start, locode) DO UPDATE SET
            n_vessels = EXCLUDED.n_vessels, n_moving = EXCLUDED.n_moving,
            n_stopped = EXCLUDED.n_stopped, n_positions = EXCLUDED.n_positions,
            mean_sog = EXCLUDED.mean_sog, updated_at = now()""",
}


def _round(v, nd=2):
    return None if v is None else round(float(v), nd)


def zone_rows(rows: list[dict], cell_km: float) -> list[dict]:
    """Ajoute l'identifiant et l'emprise géographique de chaque zone (i, j) de la grille."""
    out = []
    for r in rows:
        i, j = int(r["zone_i"]), int(r["zone_j"])
        lat_min, lon_min, lat_max, lon_max = cell_bounds(i, j, cell_km)
        out.append({
            "window_start": r["window_start"], "window_end": r["window_end"],
            "zone_id": f"{i}:{j}", "category": r["category"],
            "lat_min": lat_min, "lon_min": lon_min, "lat_max": lat_max, "lon_max": lon_max,
            "n_vessels": r["n_vessels"], "n_positions": r["n_positions"], "n_stopped": r["n_stopped"],
            "mean_sog": _round(r["mean_sog"]), "max_sog": _round(r["max_sog"]),
        })
    return out


def category_rows(rows: list[dict]) -> list[dict]:
    return [{**r, "mean_sog": _round(r["mean_sog"]), "mean_sog_moving": _round(r["mean_sog_moving"])}
            for r in rows]


def port_rows(rows: list[dict], ports) -> list[dict]:
    """Complète chaque LOCODE avec le nom et la position du port (gazetteer)."""
    out = []
    for r in rows:
        port = ports.by_locode.get(r["locode"]) if ports else None
        out.append({**r, "mean_sog": _round(r["mean_sog"]),
                    "port_name": port.name if port else None,
                    "lat": port.lat if port else None, "lon": port.lon if port else None})
    return out


class SparkSink:
    """Upserts thread-safe : les requêtes Spark appellent foreachBatch depuis plusieurs threads."""

    def __init__(self, conn, retention_hours: int = 168, prune_every_s: int = 3600):
        self.conn = conn
        self.retention_hours = retention_hours
        self.prune_every_s = prune_every_s
        self._lock = threading.Lock()
        self._next_prune = 0.0

    def init_schema(self) -> None:
        self.conn.execute(SCHEMA)

    def write(self, table: str, rows: list[dict]) -> int:
        if not rows:
            return 0
        with self._lock, self.conn.transaction(), self.conn.cursor() as cur:
            cur.executemany(_UPSERT[table], rows)
        self._maybe_prune()
        return len(rows)

    def _maybe_prune(self) -> None:
        now = time.monotonic()
        if now < self._next_prune:
            return
        self._next_prune = now + self.prune_every_s
        with self._lock:
            for table in _UPSERT:
                self.conn.execute(
                    f"DELETE FROM {table} WHERE window_start < now() - make_interval(hours => %s)",
                    (self.retention_hours,))
