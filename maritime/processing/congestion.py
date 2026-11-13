"""Détection des zones de congestion et de ralentissement sur une grille spatiale.

Pour chaque cellule (~5 km de côté) on agrège les navires actifs :
  * congestion     : plusieurs cargos / pétroliers en attente (au mouillage ou immobiles) —
                     file d'attente typique devant un port saturé ;
  * ralentissement : vitesse moyenne de la cellule nettement inférieure à sa vitesse de
                     référence (moyenne mobile exponentielle apprise au fil de l'eau) ;
  * dense          : trafic élevé mais fluide.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass

from ..ais import AT_ANCHOR, MOORED
from ..geo import cell_bounds, cell_of

# Une file d'attente de navires de charge au mouillage est le symptôme d'un port saturé.
# Les remorqueurs, pilotes ou ferries immobiles dans un bassin portuaire ne le sont pas.
QUEUE_CATEGORIES = {"cargo", "tanker"}

LEVEL_ORDER = {"normal": 0, "dense": 1, "ralentissement": 2, "congestion": 3}


@dataclass
class VesselSnapshot:
    mmsi: int
    lat: float
    lon: float
    sog: float | None
    nav_status: int
    category: str = "other"


@dataclass
class CellStat:
    cell_id: str
    lat_min: float
    lon_min: float
    lat_max: float
    lon_max: float
    n_vessels: int
    n_waiting: int
    mean_sog: float
    baseline_sog: float | None
    score: float
    level: str
    mmsis: list[int]
    place: str | None = None   # port le plus proche (renseigné par le processeur)
    weather: dict | None = None   # météo marine la plus proche (renseignée par le processeur)

    def to_dict(self) -> dict:
        return asdict(self)


class CongestionTracker:
    def __init__(self, cell_km: float = 5.0, min_vessels: int = 3, alpha: float = 0.03,
                 slowdown_ratio: float = 0.5):
        self.cell_km = cell_km
        self.min_vessels = min_vessels
        self.alpha = alpha
        self.slowdown_ratio = slowdown_ratio
        self.baseline: dict[str, float] = {}

    def cell_id(self, lat: float, lon: float) -> str:
        i, j = cell_of(lat, lon, self.cell_km)
        return f"{i}:{j}"

    def compute(self, vessels: list[VesselSnapshot], learn: bool = True) -> list[CellStat]:
        buckets: dict[tuple[int, int], list[VesselSnapshot]] = {}
        for v in vessels:
            if v.nav_status == MOORED:  # un navire à quai n'est pas un signe de congestion
                continue
            buckets.setdefault(cell_of(v.lat, v.lon, self.cell_km), []).append(v)

        stats = []
        for (i, j), members in buckets.items():
            if len(members) < 2:
                continue
            cid = f"{i}:{j}"
            sogs = [m.sog for m in members if m.sog is not None]
            mean_sog = sum(sogs) / len(sogs) if sogs else 0.0
            freight = [m for m in members if m.category in QUEUE_CATEGORIES]
            waiting = sum(1 for m in freight
                          if m.nav_status == AT_ANCHOR or (m.sog is not None and m.sog < 0.5))
            baseline = self.baseline.get(cid)
            n = len(members)

            reference = baseline if baseline and baseline > 1 else 10.0
            score = waiting + n * max(0.0, 1 - mean_sog / reference)
            if waiting >= self.min_vessels and waiting / len(freight) >= 0.6:
                level = "congestion"
            elif n >= 3 and baseline is not None and baseline >= 5 and mean_sog < self.slowdown_ratio * baseline:
                level = "ralentissement"
            elif n >= 8:
                level = "dense"
            else:
                level = "normal"

            lat_min, lon_min, lat_max, lon_max = cell_bounds(i, j, self.cell_km)
            stats.append(CellStat(cid, lat_min, lon_min, lat_max, lon_max, n, waiting,
                                  round(mean_sog, 2), round(baseline, 2) if baseline else None,
                                  round(score, 2), level, [m.mmsi for m in members][:50]))
            if learn:
                self.baseline[cid] = mean_sog if baseline is None else \
                    (1 - self.alpha) * baseline + self.alpha * mean_sog

        stats.sort(key=lambda c: (LEVEL_ORDER[c.level], c.score), reverse=True)
        return stats
