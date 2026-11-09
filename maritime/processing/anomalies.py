"""Détection d'anomalies par navire (règles métier explicables)."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field

from ..ais import UNDERWAY_STATUSES
from ..geo import haversine_nm
from .state import MINUTE_MS, VesselTrack

KIND_LABELS = {
    "POSITION_JUMP": "Saut de position",
    "SLOWDOWN": "Ralentissement brutal",
    "UNEXPECTED_STOP": "Arrêt en mer",
    "AIS_GAP": "Perte du signal AIS",
    "DELAY": "Retard probable",
    "CONGESTION": "Congestion",
}

# Catégories pour lesquelles un arrêt ou un ralentissement hors port est inhabituel
MONITORED_CATEGORIES = {"cargo", "tanker", "passenger", "other"}

MAX_PLAUSIBLE_KN = 60.0
SLOWDOWN_MIN_CRUISE_KN = 8.0
SLOWDOWN_RATIO = 0.3
STOP_MIN_DURATION = 20.0
PORT_RADIUS_NM = 5.0


@dataclass
class Alert:
    kind: str
    severity: str          # info | warning | critical
    mmsi: int | None
    ts: int
    lat: float
    lon: float
    message: str
    details: dict = field(default_factory=dict)

    @property
    def label(self) -> str:
        return KIND_LABELS.get(self.kind, self.kind)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["label"] = self.label
        return d


def check_position_jump(track: VesselTrack) -> Alert | None:
    """Vitesse implicite entre deux positions physiquement impossible (erreur GPS / usurpation)."""
    cur, prev = track.last, track.previous
    if not cur or not prev:
        return None
    dt_h = (cur.ts - prev.ts) / 3_600_000
    if dt_h <= 0:
        return None
    dist = haversine_nm(prev.lat, prev.lon, cur.lat, cur.lon)
    implied = dist / dt_h
    reported = max(cur.sog or 0, prev.sog or 0)
    if dist > 2 and implied > max(MAX_PLAUSIBLE_KN, 3 * reported + 30):
        return Alert("POSITION_JUMP", "warning", track.mmsi, cur.ts, cur.lat, cur.lon,
                     f"Déplacement de {dist:.1f} nm en {dt_h * 60:.0f} min "
                     f"(≈{implied:.0f} nœuds, vitesse déclarée {reported:.1f})",
                     {"distance_nm": round(dist, 2), "implied_kn": round(implied, 1),
                      "from": [prev.lat, prev.lon]})
    return None


def check_slowdown(track: VesselTrack, category: str, near_port: bool) -> Alert | None:
    """Passage brutal d'une vitesse de croisière à une quasi-immobilité, hors zone portuaire."""
    cur = track.last
    if not cur or cur.sog is None or near_port or category not in MONITORED_CATEGORIES:
        return None
    if cur.nav_status not in UNDERWAY_STATUSES:
        return None
    before = [p for p in track.window(cur.ts - 30 * MINUTE_MS, cur.ts - 5 * MINUTE_MS) if p.sog is not None]
    if len(before) < 3:
        return None
    cruise = sum(p.sog for p in before) / len(before)
    if cruise >= SLOWDOWN_MIN_CRUISE_KN and cur.sog < min(4.0, SLOWDOWN_RATIO * cruise):
        return Alert("SLOWDOWN", "warning", track.mmsi, cur.ts, cur.lat, cur.lon,
                     f"Vitesse passée de {cruise:.1f} à {cur.sog:.1f} nœuds hors zone portuaire",
                     {"cruise_kn": round(cruise, 1), "current_kn": cur.sog})
    return None


def check_unexpected_stop(track: VesselTrack, category: str, near_port: bool) -> Alert | None:
    """Navire immobile depuis longtemps en mer alors qu'il se déclare « en route »."""
    cur = track.last
    if not cur or near_port or category not in MONITORED_CATEGORIES:
        return None
    if cur.nav_status not in UNDERWAY_STATUSES:
        return None
    stopped = track.stopped_duration_min()
    if stopped >= STOP_MIN_DURATION:
        return Alert("UNEXPECTED_STOP", "info", track.mmsi, cur.ts, cur.lat, cur.lon,
                     f"Immobile depuis {stopped:.0f} min en mer, statut « en route »",
                     {"stopped_min": round(stopped)})
    return None


def check_ais_gap(track: VesselTrack, now_ms: int, gap_min: float) -> Alert | None:
    """Navire en mouvement qui cesse d'émettre (sortie de couverture, panne ou AIS coupé)."""
    cur = track.last
    # Au moins 3 positions : on a réellement vu le navire naviguer (évite les positions
    # anciennes de l'instantané initial)
    if not cur or cur.sog is None or cur.sog < 2 or len(track.points) < 3:
        return None
    silent = (now_ms - cur.ts) / MINUTE_MS
    if gap_min <= silent <= 3 * 60:
        return Alert("AIS_GAP", "info", track.mmsi, now_ms, cur.lat, cur.lon,
                     f"Aucun signal depuis {silent:.0f} min (dernière vitesse {cur.sog:.1f} nœuds)",
                     {"silent_min": round(silent), "last_seen": cur.ts})
    return None


def is_near_port(ports, lat: float, lon: float, radius_nm: float = PORT_RADIUS_NM) -> bool:
    return bool(ports and ports.nearest(lat, lon, radius_nm))
