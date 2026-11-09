"""Processeur de flux : consomme Kafka, détecte congestion / retards / anomalies.

    python -m maritime.processor

Entrées  : ais.positions, ais.static
Sorties  : PostgreSQL (état temps réel + historique) ; topics ais.alerts et ais.congestion
"""
from __future__ import annotations

import logging
import signal
import time
from datetime import datetime, timezone

from .ais import COMMERCIAL_CATEGORIES, eta_to_datetime, flag_of, ship_category
from .config import settings
from .db import Writer, connect, init_schema
from .kafka_io import JsonProducer, decode, ensure_topics, json_consumer
from .ports import load_ports
from .processing import anomalies as an
from .processing.congestion import CongestionTracker, VesselSnapshot
from .processing.eta import ALERT_MAX_DISTANCE_NM, estimate
from .processing.state import MINUTE_MS, Point, VesselTrack

log = logging.getLogger("processor")

PERSISTENT_KINDS = {"UNEXPECTED_STOP", "AIS_GAP", "DELAY", "CONGESTION"}


def now_ms() -> int:
    return int(time.time() * 1000)


class Processor:
    def __init__(self, writer: Writer | None, producer: JsonProducer | None, ports=None):
        self.writer = writer
        self.producer = producer
        self.ports = ports
        self.tracks: dict[int, VesselTrack] = {}
        self.meta: dict[int, dict] = {}
        self.congestion = CongestionTracker(settings.cell_km, settings.congestion_min_vessels)
        self.cooldown: dict[tuple, int] = {}
        self.active: set[tuple] = set()      # conditions persistantes en cours
        self.pending_positions: list[dict] = []
        self.pending_vessels: dict[int, dict] = {}
        self.pending_alerts: list[dict] = []
        self.counters = {"positions": 0, "static": 0, "alerts": 0}
        self.last_lag_s: float | None = None

    # ------------------------------------------------------------------ helpers
    def _vessel_row(self, mmsi: int, **fields) -> dict:
        iso, country = flag_of(mmsi)
        row = {"mmsi": mmsi, "name": None, "callsign": None, "imo": None, "ship_type": None,
               "category": "other", "country_code": iso, "country": country, "destination": None,
               "eta": None, "draught": None, "length": None, "width": None}
        row.update(fields)
        return row

    def category(self, mmsi: int) -> str:
        return self.meta.get(mmsi, {}).get("category", "other")

    def emit(self, alert: an.Alert, key: tuple | None = None) -> None:
        """Publie une alerte (avec anti-rebond) dans Kafka et la met en file pour la base."""
        key = key or (alert.mmsi, alert.kind)
        last = self.cooldown.get(key)
        if last and alert.ts - last < settings.alert_cooldown_min * MINUTE_MS:
            return
        self.cooldown[key] = alert.ts
        payload = alert.to_dict()
        meta = self.meta.get(alert.mmsi or -1, {})
        payload["vessel_name"] = meta.get("name")
        self.pending_alerts.append(payload)
        self.counters["alerts"] += 1
        if self.producer:
            self.producer.send(settings.topics.alerts, payload, key=alert.mmsi)

    def set_condition(self, key: tuple, alert: an.Alert | None) -> None:
        """Alerte uniquement à l'apparition d'une condition persistante, réarmée à sa disparition."""
        if alert is None:
            self.active.discard(key)
        elif key not in self.active:
            self.active.add(key)
            self.emit(alert, key)

    # ----------------------------------------------------------------- handlers
    def handle_static(self, msg: dict) -> None:
        mmsi = int(msg["mmsi"])
        cat = ship_category(msg.get("ship_type"))
        eta_dt = eta_to_datetime(msg.get("eta"), datetime.now(timezone.utc))
        self.meta[mmsi] = {"name": msg.get("name"), "category": cat,
                           "destination": msg.get("destination"), "eta": eta_dt}
        self.pending_vessels[mmsi] = self._vessel_row(
            mmsi, name=msg.get("name"), callsign=msg.get("callsign"), imo=msg.get("imo"),
            ship_type=msg.get("ship_type"), category=cat, destination=msg.get("destination"),
            eta=eta_dt, draught=msg.get("draught"), length=msg.get("length"), width=msg.get("width"))
        self.counters["static"] += 1

    def handle_position(self, msg: dict) -> None:
        mmsi = int(msg["mmsi"])
        track = self.tracks.get(mmsi)
        if track is None:
            track = self.tracks[mmsi] = VesselTrack(mmsi)
            if mmsi not in self.meta:
                self.meta[mmsi] = {"category": "other"}
                self.pending_vessels.setdefault(mmsi, self._vessel_row(mmsi))
        if not track.add(Point.from_msg(msg)):
            return
        self.counters["positions"] += 1
        if msg.get("ingested_at"):
            self.last_lag_s = (now_ms() - msg["ingested_at"]) / 1000

        cat = self.category(mmsi)
        cur = track.last
        near_port = an.is_near_port(self.ports, cur.lat, cur.lon) if cat in an.MONITORED_CATEGORIES else True

        jump = an.check_position_jump(track)
        if jump:
            self.emit(jump)
        slowdown = an.check_slowdown(track, cat, near_port)
        if slowdown:
            self.emit(slowdown)
        self.set_condition((mmsi, "UNEXPECTED_STOP"), an.check_unexpected_stop(track, cat, near_port))
        self.active.discard((mmsi, "AIS_GAP"))  # le navire émet de nouveau

        self.pending_positions.append({**msg, "avg_sog_30m": track.avg_sog(30)})

    # -------------------------------------------------------------------- ticks
    def flush(self) -> None:
        if not self.writer:
            self.pending_positions.clear(); self.pending_vessels.clear(); self.pending_alerts.clear()
            return
        # Les fiches navires d'abord (clé étrangère logique des positions)
        self.writer.upsert_vessels(list(self.pending_vessels.values()))
        self.writer.insert_positions(self.pending_positions)
        self.writer.insert_alerts(self.pending_alerts)
        self.pending_vessels.clear(); self.pending_positions.clear(); self.pending_alerts.clear()

    def tick(self) -> None:
        """Traitements périodiques (chaque minute) sur l'ensemble de la flotte active."""
        t0 = time.monotonic()
        now = now_ms()
        now_dt = datetime.now(timezone.utc)
        active_since = now - settings.active_window_min * MINUTE_MS

        snapshots, eta_rows = [], []
        for mmsi, track in self.tracks.items():
            cur = track.last
            if cur is None:
                continue
            # 1. Pertes de signal AIS
            self.set_condition((mmsi, "AIS_GAP"), an.check_ais_gap(track, now, settings.ais_gap_min))
            if cur.ts < active_since:
                continue
            cat = self.category(mmsi)
            snapshots.append(VesselSnapshot(mmsi, cur.lat, cur.lon, cur.sog, cur.nav_status, cat))

            # 2. Estimation d'arrivée / retards
            meta = self.meta.get(mmsi, {})
            port = self.ports.resolve(meta.get("destination")) if self.ports else None
            est = estimate(now_dt, cur.lat, cur.lon, track.avg_sog(30), cat, port, meta.get("eta"))
            eta_rows.append((mmsi, est))
            delayed = est and est["status"] in ("retard_probable", "retard_important") \
                and cat in COMMERCIAL_CATEGORIES and est["remaining_nm"] <= ALERT_MAX_DISTANCE_NM
            self.set_condition((mmsi, "DELAY"), an.Alert(
                "DELAY", "critical" if est["status"] == "retard_important" else "warning",
                mmsi, now, cur.lat, cur.lon,
                f"{est['status_label']} vers {est['dest_name']} : +{est['delay_min']} min "
                f"(reste {est['remaining_nm']:.0f} nm)", est) if delayed else None)

        # 3. Congestion / ralentissement par zone
        cells = self.congestion.compute(snapshots)
        flagged = {c.cell_id for c in cells if c.level in ("congestion", "ralentissement")}
        for c in cells:
            if c.level in ("congestion", "ralentissement"):
                lat, lon = (c.lat_min + c.lat_max) / 2, (c.lon_min + c.lon_max) / 2
                port = self.ports.nearest(lat, lon, 25) if self.ports else None
                c.place = port[0].name if port else None
                where = f"près de {c.place}" if port else f"({lat:.2f}, {lon:.2f})"
                self.set_condition(("zone", c.cell_id, c.level), an.Alert(
                    "CONGESTION", "critical" if c.level == "congestion" else "warning", None, now,
                    lat, lon,
                    f"{c.level.capitalize()} {where} : {c.n_vessels} navires, "
                    f"{c.n_waiting} en attente, vitesse moyenne {c.mean_sog:.1f} nœuds",
                    {"cell_id": c.cell_id, "level": c.level, "n_vessels": c.n_vessels,
                     "mean_sog": c.mean_sog, "baseline_sog": c.baseline_sog}))
        for key in [k for k in self.active if k[0] == "zone" and k[1] not in flagged]:
            self.active.discard(key)

        if self.producer:
            self.producer.send(settings.topics.congestion, {
                "ts": now, "cells": [c.to_dict() for c in cells if c.level != "normal"]})
            self.producer.flush(5)

        # 4. Purge des navires disparus depuis longtemps
        stale = now - 6 * 3600 * 1000
        for mmsi in [m for m, t in self.tracks.items() if t.last and t.last.ts < stale]:
            del self.tracks[mmsi]

        if self.writer:
            self.flush()
            self.writer.update_eta_estimates(eta_rows)
            self.writer.replace_congestion([c.to_dict() for c in cells])
            self.writer.record_stats(self.counters["positions"], self.counters["static"],
                                     self.counters["alerts"], self.last_lag_s)
        log.info("Tick : %d navires actifs, %d zones (%d signalées), %d pos / %d fiches / %d alertes "
                 "depuis le dernier tick, %.1f s", len(snapshots), len(cells), len(flagged),
                 self.counters["positions"], self.counters["static"], self.counters["alerts"],
                 time.monotonic() - t0)
        self.counters = {"positions": 0, "static": 0, "alerts": 0}

    def warm_start(self) -> None:
        """Recharge l'état récent depuis la base pour ne pas perdre le contexte au redémarrage."""
        if not self.writer:
            return
        vessels, positions, baselines, alerts = self.writer.load_recent()
        for mmsi, name, _stype, cat, dest, eta in vessels:
            self.meta[mmsi] = {"name": name, "category": cat, "destination": dest, "eta": eta}
        for mmsi, ts, lat, lon, sog, cog, hdg, ns in positions:
            self.tracks.setdefault(mmsi, VesselTrack(mmsi)).add(Point(ts, lat, lon, sog, cog, hdg, ns or 15))
        for cell_id, sog in baselines:
            self.congestion.baseline[cell_id] = float(sog)
        # Anti-rebond : une condition déjà signalée avant le redémarrage n'est pas réémise
        for mmsi, kind, ts, cell_id, level in alerts:
            key = ("zone", cell_id, level) if kind == "CONGESTION" else (mmsi, kind)
            self.cooldown[key] = ts
            if kind in PERSISTENT_KINDS:
                self.active.add(key)
        log.info("Reprise : %d navires, %d positions, %d zones de référence",
                 len(vessels), len(positions), len(baselines))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    ensure_topics()
    conn = connect(settings.database_url)
    init_schema(conn)
    writer = Writer(conn)
    ports = load_ports(settings.ports_cache, settings.digitraffic_user)
    proc = Processor(writer, JsonProducer(), ports)
    proc.warm_start()

    consumer = json_consumer("vigie-processor", [settings.topics.positions, settings.topics.static])
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    next_tick = time.monotonic() + settings.tick_interval_s
    next_flush = time.monotonic() + 2
    next_prune = time.monotonic() + 600
    try:
        while running:
            for msg in consumer.consume(num_messages=500, timeout=1.0):
                if msg.error():
                    log.warning("Erreur Kafka : %s", msg.error())
                    continue
                data = decode(msg)
                if not data:
                    continue
                if msg.topic() == settings.topics.static:
                    proc.handle_static(data)
                else:
                    proc.handle_position(data)
            t = time.monotonic()
            if t >= next_flush:
                proc.flush()
                next_flush = t + 2
            if t >= next_tick:
                proc.tick()
                next_tick = t + settings.tick_interval_s
            if t >= next_prune:
                writer.prune(settings.retention_hours)
                next_prune = t + 3600
    finally:
        consumer.close()
        proc.flush()


if __name__ == "__main__":
    main()
