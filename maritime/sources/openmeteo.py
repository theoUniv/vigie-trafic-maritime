"""Open-Meteo : vent, visibilité et état de la mer sur une grille de points (open data, sans clé).

- Prévisions météo : https://api.open-meteo.com/v1/forecast (vent à 10 m, rafales, visibilité)
- Météo marine     : https://marine-api.open-meteo.com/v1/marine (vagues, température de l'eau)

Données Open-Meteo sous licence CC BY 4.0. Une requête accepte plusieurs points (latitudes et
longitudes séparées par des virgules) ; les points à terre sont écartés grâce à la réponse
marine, qui n'a pas de vagues sur la terre ferme.
"""
from __future__ import annotations

import calendar
import json
import logging
import time
from pathlib import Path

import requests

log = logging.getLogger(__name__)

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
MARINE_URL = "https://marine-api.open-meteo.com/v1/marine"
WEATHER_VARS = "wind_speed_10m,wind_direction_10m,wind_gusts_10m,visibility,weather_code"
MARINE_VARS = "wave_height,wave_direction,wave_period,sea_surface_temperature"
CHUNK = 100   # points par requête


def build_grid(bbox: str, step: str) -> list[tuple[float, float]]:
    """Grille régulière « sud,ouest,nord,est » au pas « dlat,dlon » (degrés)."""
    s, w, n, e = (float(x) for x in bbox.split(","))
    dlat, dlon = (float(x) for x in step.split(","))
    points, lat = [], s
    while lat <= n + 1e-9:
        lon = w
        while lon <= e + 1e-9:
            points.append((round(lat, 3), round(lon, 3)))
            lon += dlon
        lat += dlat
    return points


def _as_list(payload) -> list[dict]:
    """Un seul point : un objet ; plusieurs points : une liste."""
    return payload if isinstance(payload, list) else [payload]


def _parse_ts(iso: str | None) -> int | None:
    if not iso:
        return None
    return calendar.timegm(time.strptime(iso, "%Y-%m-%dT%H:%M")) * 1000   # heure GMT


def merge_observations(points: list[tuple[float, float]], weather: list[dict],
                       marine: list[dict]) -> list[dict]:
    """Associe les deux réponses point par point (même ordre que la requête)."""
    out = []
    for (lat, lon), wx, mx in zip(points, weather, marine):
        cw, cm = wx.get("current") or {}, mx.get("current") or {}
        if cm.get("wave_height") is None:   # point à terre (ou hors du modèle de vagues)
            continue
        out.append({
            "lat": lat, "lon": lon,
            "ts": _parse_ts(cw.get("time") or cm.get("time")),
            "wind_kn": cw.get("wind_speed_10m"),
            "wind_dir": cw.get("wind_direction_10m"),
            "gust_kn": cw.get("wind_gusts_10m"),
            "visibility_m": cw.get("visibility"),
            "weather_code": cw.get("weather_code"),
            "wave_m": cm.get("wave_height"),
            "wave_dir": cm.get("wave_direction"),
            "wave_period_s": cm.get("wave_period"),
            "sea_temp_c": cm.get("sea_surface_temperature"),
            "source": "open-meteo",
        })
    return out


class OpenMeteoSource:
    """Chaque point d'une requête compte comme un appel pour Open-Meteo (gratuit : 600 par minute,
    10 000 par jour). On étale donc les requêtes, on n'interroge le vent que sur les points en mer,
    et la liste de ces points est mémorisée pour ne pas rebalayer la grille à chaque démarrage."""

    def __init__(self, points: list[tuple[float, float]], cache_path: str | None = None,
                 timeout_s: float = 30, calls_per_min: int = 400):
        self.cache_path = Path(cache_path) if cache_path else None
        self.timeout_s = timeout_s
        self.pause_per_point = 60 / calls_per_min
        self.session = requests.Session()
        self.session.headers["User-Agent"] = "vigie-maritime (projet open data)"
        self.grid = points
        cached = self._load_cache()
        self.points = cached or points
        self._sea_known = cached is not None

    def _load_cache(self) -> list[tuple[float, float]] | None:
        if not self.cache_path or not self.cache_path.exists():
            return None
        try:
            data = json.loads(self.cache_path.read_text())
            if data.get("grid") != len(self.grid):     # grille modifiée : on rebalaye
                return None
            return [tuple(p) for p in data["sea_points"]]
        except (ValueError, KeyError, OSError):
            return None

    def _save_cache(self) -> None:
        if self.cache_path:
            try:
                self.cache_path.parent.mkdir(parents=True, exist_ok=True)
                self.cache_path.write_text(json.dumps({"grid": len(self.grid), "sea_points": self.points}))
            except OSError as exc:
                log.warning("Cache des points en mer non écrit : %s", exc)

    def _get(self, url: str, chunk: list[tuple[float, float]], variables: str, extra: dict) -> list[dict]:
        params = {"latitude": ",".join(str(p[0]) for p in chunk),
                  "longitude": ",".join(str(p[1]) for p in chunk),
                  "current": variables, "timezone": "GMT", **extra}
        r = self.session.get(url, params=params, timeout=self.timeout_s)
        r.raise_for_status()
        data = _as_list(r.json())
        if len(data) != len(chunk):
            raise ValueError(f"Open-Meteo : {len(data)} réponses pour {len(chunk)} points")
        time.sleep(len(chunk) * self.pause_per_point)   # reste sous la limite par minute
        return data

    def fetch(self) -> list[dict]:
        """Une observation par point en mer : vagues d'abord, puis vent sur les seuls points en mer."""
        obs = []
        for i in range(0, len(self.points), CHUNK):
            chunk = self.points[i:i + CHUNK]
            marine = self._get(MARINE_URL, chunk, MARINE_VARS, {})
            sea = [(p, m) for p, m in zip(chunk, marine) if (m.get("current") or {}).get("wave_height") is not None]
            if not sea:
                continue
            sea_points = [p for p, _ in sea]
            weather = self._get(FORECAST_URL, sea_points, WEATHER_VARS, {"wind_speed_unit": "kn"})
            obs.extend(merge_observations(sea_points, weather, [m for _, m in sea]))
        if not self._sea_known and obs:
            self.points = [(o["lat"], o["lon"]) for o in obs]
            self._sea_known = True
            self._save_cache()
            log.info("Grille météo : %d points en mer sur %d", len(self.points), len(self.grid))
        return obs
