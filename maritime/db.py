"""Schéma PostgreSQL et écritures du processeur de flux."""
from __future__ import annotations

import json
import logging
import time

import psycopg

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS vessels (
    mmsi          BIGINT PRIMARY KEY,
    name          TEXT,
    callsign      TEXT,
    imo           BIGINT,
    ship_type     INT,
    category      TEXT NOT NULL DEFAULT 'other',
    country_code  TEXT,
    country       TEXT,
    destination   TEXT,
    eta           TIMESTAMPTZ,
    draught       REAL,
    length        REAL,
    width         REAL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS vessels_name_idx ON vessels (lower(name));

-- Historique des positions (une ligne par message AIS reçu)
CREATE TABLE IF NOT EXISTS positions (
    mmsi        BIGINT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL,
    lat         DOUBLE PRECISION NOT NULL,
    lon         DOUBLE PRECISION NOT NULL,
    sog         REAL,
    cog         REAL,
    heading     SMALLINT,
    nav_status  SMALLINT,
    PRIMARY KEY (mmsi, ts)
);
CREATE INDEX IF NOT EXISTS positions_ts_idx ON positions (ts);

-- Dernier état connu de chaque navire (alimente la carte temps réel)
CREATE TABLE IF NOT EXISTS vessel_state (
    mmsi         BIGINT PRIMARY KEY,
    ts           TIMESTAMPTZ NOT NULL,
    lat          DOUBLE PRECISION NOT NULL,
    lon          DOUBLE PRECISION NOT NULL,
    sog          REAL,
    cog          REAL,
    heading      SMALLINT,
    nav_status   SMALLINT,
    avg_sog_30m  REAL,
    eta_estimate JSONB
);
CREATE INDEX IF NOT EXISTS vessel_state_ts_idx ON vessel_state (ts);

CREATE TABLE IF NOT EXISTS anomalies (
    id        BIGSERIAL PRIMARY KEY,
    ts        TIMESTAMPTZ NOT NULL,
    mmsi      BIGINT,
    kind      TEXT NOT NULL,
    severity  TEXT NOT NULL,
    lat       DOUBLE PRECISION,
    lon       DOUBLE PRECISION,
    message   TEXT,
    details   JSONB
);
CREATE INDEX IF NOT EXISTS anomalies_ts_idx ON anomalies (ts DESC);
CREATE INDEX IF NOT EXISTS anomalies_mmsi_idx ON anomalies (mmsi, ts DESC);

-- Zones (cellules) de la grille : état courant et historique pour l'analyse
CREATE TABLE IF NOT EXISTS congestion_current (
    cell_id       TEXT PRIMARY KEY,
    ts            TIMESTAMPTZ NOT NULL,
    lat_min DOUBLE PRECISION, lon_min DOUBLE PRECISION,
    lat_max DOUBLE PRECISION, lon_max DOUBLE PRECISION,
    n_vessels     INT,
    n_waiting     INT,
    mean_sog      REAL,
    baseline_sog  REAL,
    score         REAL,
    level         TEXT,
    mmsis         BIGINT[]
);
ALTER TABLE congestion_current ADD COLUMN IF NOT EXISTS place TEXT;
CREATE TABLE IF NOT EXISTS congestion_history (
    ts         TIMESTAMPTZ NOT NULL,
    cell_id    TEXT NOT NULL,
    lat        DOUBLE PRECISION,
    lon        DOUBLE PRECISION,
    n_vessels  INT,
    n_waiting  INT,
    mean_sog   REAL,
    score      REAL,
    level      TEXT
);
CREATE INDEX IF NOT EXISTS congestion_history_ts_idx ON congestion_history (ts);

-- Débit d'ingestion (supervision du pipeline)
CREATE TABLE IF NOT EXISTS ingest_stats (
    ts          TIMESTAMPTZ PRIMARY KEY,
    positions   INT,
    static_msgs INT,
    alerts      INT,
    lag_s       REAL
);
"""


def connect(url: str, retries: int = 30) -> psycopg.Connection:
    for attempt in range(retries):
        try:
            conn = psycopg.connect(url, autocommit=True)
            return conn
        except psycopg.OperationalError as exc:
            if attempt == retries - 1:
                raise
            log.info("PostgreSQL pas encore prêt (%s)...", exc)
            time.sleep(2)
    raise RuntimeError("unreachable")


def init_schema(conn: psycopg.Connection) -> None:
    conn.execute(SCHEMA)


class Writer:
    """Écritures groupées du processeur."""

    def __init__(self, conn: psycopg.Connection):
        self.conn = conn

    def upsert_vessels(self, rows: list[dict]) -> None:
        if not rows:
            return
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO vessels (mmsi, name, callsign, imo, ship_type, category, country_code,
                                     country, destination, eta, draught, length, width, updated_at)
                VALUES (%(mmsi)s, %(name)s, %(callsign)s, %(imo)s, %(ship_type)s, %(category)s,
                        %(country_code)s, %(country)s, %(destination)s, %(eta)s, %(draught)s,
                        %(length)s, %(width)s, now())
                ON CONFLICT (mmsi) DO UPDATE SET
                    name = COALESCE(EXCLUDED.name, vessels.name),
                    callsign = COALESCE(EXCLUDED.callsign, vessels.callsign),
                    imo = COALESCE(EXCLUDED.imo, vessels.imo),
                    ship_type = COALESCE(EXCLUDED.ship_type, vessels.ship_type),
                    category = CASE WHEN EXCLUDED.ship_type IS NULL THEN vessels.category
                                    ELSE EXCLUDED.category END,
                    destination = COALESCE(EXCLUDED.destination, vessels.destination),
                    eta = COALESCE(EXCLUDED.eta, vessels.eta),
                    draught = COALESCE(EXCLUDED.draught, vessels.draught),
                    length = COALESCE(EXCLUDED.length, vessels.length),
                    width = COALESCE(EXCLUDED.width, vessels.width),
                    updated_at = now()
                """,
                rows,
            )

    def insert_positions(self, rows: list[dict]) -> None:
        if not rows:
            return
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO positions (mmsi, ts, lat, lon, sog, cog, heading, nav_status)
                VALUES (%(mmsi)s, to_timestamp(%(ts)s / 1000.0), %(lat)s, %(lon)s, %(sog)s,
                        %(cog)s, %(heading)s, %(nav_status)s)
                ON CONFLICT DO NOTHING
                """,
                rows,
            )
            cur.executemany(
                """
                INSERT INTO vessel_state (mmsi, ts, lat, lon, sog, cog, heading, nav_status, avg_sog_30m)
                VALUES (%(mmsi)s, to_timestamp(%(ts)s / 1000.0), %(lat)s, %(lon)s, %(sog)s,
                        %(cog)s, %(heading)s, %(nav_status)s, %(avg_sog_30m)s)
                ON CONFLICT (mmsi) DO UPDATE SET
                    ts = EXCLUDED.ts, lat = EXCLUDED.lat, lon = EXCLUDED.lon, sog = EXCLUDED.sog,
                    cog = EXCLUDED.cog, heading = EXCLUDED.heading, nav_status = EXCLUDED.nav_status,
                    avg_sog_30m = EXCLUDED.avg_sog_30m
                WHERE vessel_state.ts <= EXCLUDED.ts
                """,
                rows,
            )

    def update_eta_estimates(self, rows: list[tuple[int, dict | None]]) -> None:
        if not rows:
            return
        with self.conn.cursor() as cur:
            cur.executemany(
                "UPDATE vessel_state SET eta_estimate = %s WHERE mmsi = %s",
                [(json.dumps(est) if est else None, mmsi) for mmsi, est in rows],
            )

    def insert_alerts(self, alerts: list[dict]) -> None:
        if not alerts:
            return
        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO anomalies (ts, mmsi, kind, severity, lat, lon, message, details)
                VALUES (to_timestamp(%(ts)s / 1000.0), %(mmsi)s, %(kind)s, %(severity)s, %(lat)s,
                        %(lon)s, %(message)s, %(details)s)
                """,
                [{**a, "details": json.dumps(a.get("details") or {})} for a in alerts],
            )

    def replace_congestion(self, cells: list[dict]) -> None:
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.execute("DELETE FROM congestion_current")
            if not cells:
                return
            cur.executemany(
                """
                INSERT INTO congestion_current (cell_id, ts, lat_min, lon_min, lat_max, lon_max,
                    n_vessels, n_waiting, mean_sog, baseline_sog, score, level, mmsis, place)
                VALUES (%(cell_id)s, now(), %(lat_min)s, %(lon_min)s, %(lat_max)s, %(lon_max)s,
                    %(n_vessels)s, %(n_waiting)s, %(mean_sog)s, %(baseline_sog)s, %(score)s,
                    %(level)s, %(mmsis)s, %(place)s)
                """,
                cells,
            )
            cur.executemany(
                """
                INSERT INTO congestion_history (ts, cell_id, lat, lon, n_vessels, n_waiting,
                                                mean_sog, score, level)
                VALUES (now(), %(cell_id)s, (%(lat_min)s + %(lat_max)s) / 2,
                        (%(lon_min)s + %(lon_max)s) / 2, %(n_vessels)s, %(n_waiting)s,
                        %(mean_sog)s, %(score)s, %(level)s)
                """,
                cells,
            )

    def record_stats(self, positions: int, static_msgs: int, alerts: int, lag_s: float | None) -> None:
        self.conn.execute(
            "INSERT INTO ingest_stats VALUES (now(), %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (positions, static_msgs, alerts, lag_s),
        )

    def prune(self, retention_hours: int) -> None:
        for table in ("positions", "congestion_history", "ingest_stats"):
            self.conn.execute(f"DELETE FROM {table} WHERE ts < now() - make_interval(hours => %s)",
                              (retention_hours,))
        self.conn.execute("DELETE FROM anomalies WHERE ts < now() - make_interval(hours => %s)",
                          (retention_hours * 4,))
        self.conn.execute("DELETE FROM vessel_state WHERE ts < now() - interval '24 hours'")

    # --- Reprise après redémarrage -------------------------------------------------
    def load_recent(self, hours: int = 3):
        vessels = self.conn.execute(
            "SELECT mmsi, name, ship_type, category, destination, eta FROM vessels").fetchall()
        positions = self.conn.execute(
            """SELECT mmsi, (extract(epoch FROM ts) * 1000)::bigint, lat, lon, sog, cog, heading,
                      nav_status
               FROM positions WHERE ts > now() - make_interval(hours => %s) ORDER BY ts""",
            (hours,)).fetchall()
        baselines = self.conn.execute(
            """SELECT cell_id, avg(mean_sog) FROM congestion_history
               WHERE ts > now() - interval '24 hours' GROUP BY cell_id""").fetchall()
        alerts = self.conn.execute(
            """SELECT mmsi, kind, (extract(epoch FROM ts) * 1000)::bigint, details->>'cell_id',
                      details->>'level'
               FROM anomalies WHERE ts > now() - interval '3 hours' ORDER BY ts""").fetchall()
        return vessels, positions, baselines, alerts
