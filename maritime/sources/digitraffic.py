"""Source Digitraffic (Agence finlandaise des transports — open data, licence CC BY 4.0).

API REST publique, sans clé : positions AIS temps réel de la mer Baltique
(réseau de stations côtières finlandaises). Documentation :
https://www.digitraffic.fi/en/marine-traffic/
"""
from __future__ import annotations

import logging
import time
from collections.abc import Iterator

import requests

from ..ais import unpack_digitraffic_eta

log = logging.getLogger(__name__)

BASE = "https://meri.digitraffic.fi/api/ais/v1"


def _f(value, invalid=None):
    """Valeur numérique ou None si absente / sentinelle AIS 'non disponible'."""
    if value is None or value == invalid:
        return None
    return value


def normalize_location(feature: dict) -> dict | None:
    props = feature.get("properties") or {}
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates")
    mmsi = feature.get("mmsi") or props.get("mmsi")
    if not coords or not mmsi:
        return None
    lon, lat = coords[:2]
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None
    return {
        "mmsi": int(mmsi),
        "ts": int(props.get("timestampExternal") or time.time() * 1000),
        "lat": float(lat),
        "lon": float(lon),
        "sog": _f(props.get("sog"), 102.3),
        "cog": _f(props.get("cog"), 360),
        "heading": _f(props.get("heading"), 511),
        "nav_status": props.get("navStat", 15),
        "rot": _f(props.get("rot"), -128),
        "source": "digitraffic",
    }


def normalize_vessel(v: dict) -> dict:
    dims = [v.get(f"referencePoint{k}") or 0 for k in "ABCD"]
    return {
        "mmsi": int(v["mmsi"]),
        "ts": int(v.get("timestamp") or time.time() * 1000),
        "name": (v.get("name") or "").strip() or None,
        "callsign": (v.get("callSign") or "").strip() or None,
        "imo": v.get("imo") or None,
        "ship_type": v.get("shipType"),
        "destination": (v.get("destination") or "").strip() or None,
        "eta": unpack_digitraffic_eta(v.get("eta")),
        "draught": (v["draught"] / 10) if v.get("draught") else None,  # 1/10 m -> m
        "length": (dims[0] + dims[1]) or None,
        "width": (dims[2] + dims[3]) or None,
        "source": "digitraffic",
    }


class DigitrafficSource:
    def __init__(self, user_agent: str, poll_interval_s: int = 60, static_refresh_s: int = 600):
        self.session = requests.Session()
        self.session.headers.update({"Digitraffic-User": user_agent, "Accept-Encoding": "gzip"})
        self.poll_interval_s = poll_interval_s
        self.static_refresh_s = static_refresh_s
        self._last_pos_ts: dict[int, int] = {}
        self._last_static_poll_ms: int | None = None

    def _get(self, path: str, **params):
        r = self.session.get(f"{BASE}{path}", params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    def fetch_positions(self) -> list[dict]:
        # Premier appel : instantané complet (dernière position de chaque navire sur 24 h).
        # Ensuite : uniquement les positions reçues depuis le dernier appel (+ marge), ce qui
        # évite l'instantané global mis en cache plusieurs minutes par le CDN.
        params = {}
        if self._last_pos_ts:
            params["from"] = int(time.time() * 1000) - (self.poll_interval_s + 120) * 1000
        data = self._get("/locations", **params)
        out = []
        for feat in data.get("features", []):
            msg = normalize_location(feat)
            # On ne republie que les positions nouvelles (dédoublonnage par horodatage)
            if msg and msg["ts"] > self._last_pos_ts.get(msg["mmsi"], 0):
                self._last_pos_ts[msg["mmsi"]] = msg["ts"]
                out.append(msg)
        return out

    def fetch_static(self, force: bool = False) -> list[dict]:
        now_ms = int(time.time() * 1000)
        if not force and self._last_static_poll_ms and \
                now_ms - self._last_static_poll_ms < self.static_refresh_s * 1000:
            return []
        # Premier appel : 24 h d'historique ; ensuite uniquement les mises à jour
        since = (self._last_static_poll_ms or now_ms - 24 * 3600 * 1000) - 60_000
        vessels = self._get("/vessels", **{"from": since})
        self._last_static_poll_ms = now_ms
        return [normalize_vessel(v) for v in vessels if v.get("mmsi")]

    def snapshots(self) -> Iterator[dict]:
        while True:
            started = time.monotonic()
            batch = {"positions": [], "static": []}
            try:
                batch["static"] = self.fetch_static()
                batch["positions"] = self.fetch_positions()
            except requests.RequestException as exc:
                log.error("Erreur API Digitraffic : %s", exc)
            yield batch
            time.sleep(max(0.0, self.poll_interval_s - (time.monotonic() - started)))
