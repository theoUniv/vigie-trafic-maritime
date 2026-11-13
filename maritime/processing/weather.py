"""Météo marine : état de la mer et recherche du point météo le plus proche (logique pure).

Les observations viennent d'Open-Meteo (vent, rafales, visibilité, vagues) sur une grille de
points en mer. Elles servent à expliquer les ralentissements : une zone qui ralentit par vent
de force 7 n'a pas la même signification qu'une zone qui ralentit par mer calme.
"""
from __future__ import annotations

from ..geo import haversine_nm

KM_PER_NM = 1.852

# Seuils : échelle de Beaufort pour le vent (nœuds), échelle de Douglas pour la mer (mètres)
GALE_KN = 34          # force 8 : coup de vent
STRONG_WIND_KN = 22   # force 6 : vent frais, avis de grand frais au-delà
ROUGH_SEA_M = 2.5     # mer forte (Douglas 5)
VERY_ROUGH_SEA_M = 4.0
FOG_VISIBILITY_M = 1000

LEVELS = ["calme", "agitée", "difficile", "dangereuse"]

WMO_LABELS = {
    0: "ciel clair", 1: "peu nuageux", 2: "partiellement nuageux", 3: "couvert",
    45: "brouillard", 48: "brouillard givrant",
    51: "bruine", 53: "bruine", 55: "bruine forte", 56: "bruine verglaçante", 57: "bruine verglaçante",
    61: "pluie faible", 63: "pluie", 65: "pluie forte", 66: "pluie verglaçante", 67: "pluie verglaçante",
    71: "neige faible", 73: "neige", 75: "neige forte", 77: "grains de neige",
    80: "averses", 81: "averses", 82: "averses violentes", 85: "averses de neige", 86: "averses de neige",
    95: "orage", 96: "orage avec grêle", 99: "orage avec grêle",
}


def beaufort(wind_kn: float | None) -> int | None:
    if wind_kn is None:
        return None
    for force, limit in enumerate((1, 4, 7, 11, 17, 22, 28, 34, 41, 48, 56, 64)):
        if wind_kn < limit:
            return force
    return 12


def _fmt(v: float) -> str:
    return f"{v:.1f}".replace(".", ",")


def sea_state(w: dict | None) -> dict | None:
    """Résume une observation : niveau 0 (calme) à 3 (dangereuse), libellé et causes."""
    if not w:
        return None
    wind, gust, wave = w.get("wind_kn"), w.get("gust_kn"), w.get("wave_m")
    vis, code = w.get("visibility_m"), w.get("weather_code")
    level, causes = 0, []
    if wind is not None:
        if wind >= GALE_KN or (gust or 0) >= GALE_KN + 10:
            level = 3
            causes.append(f"coup de vent ({_fmt(wind)} nd, force {beaufort(wind)})")
        elif wind >= STRONG_WIND_KN:
            level = max(level, 2)
            causes.append(f"vent fort ({_fmt(wind)} nd, force {beaufort(wind)})")
        elif wind >= 17:
            level = max(level, 1)
    if wave is not None:
        if wave >= VERY_ROUGH_SEA_M:
            level = 3
            causes.append(f"très forte mer ({_fmt(wave)} m)")
        elif wave >= ROUGH_SEA_M:
            level = max(level, 2)
            causes.append(f"mer forte ({_fmt(wave)} m)")
        elif wave >= 1.25:
            level = max(level, 1)
    if vis is not None and vis < FOG_VISIBILITY_M:
        level = max(level, 2)
        causes.append(f"visibilité réduite ({vis / 1000:.1f} km)".replace(".", ","))
    if code in (95, 96, 99):
        level = max(level, 2)
        causes.append("orage")

    parts = []
    if wind is not None:
        parts.append(f"vent {_fmt(wind)} nd")
    if wave is not None:
        parts.append(f"vagues {_fmt(wave)} m")
    return {
        "level": level,
        "label": LEVELS[level],
        "summary": ", ".join(parts) or "pas de mesure",
        "causes": causes,
        "sky": WMO_LABELS.get(code),
        "beaufort": beaufort(wind),
    }


class WeatherIndex:
    """Dernière observation par point de grille, et recherche du point le plus proche."""

    def __init__(self, max_km: float = 120):
        self.max_nm = max_km / KM_PER_NM
        self.points: dict[tuple[float, float], dict] = {}

    def update(self, obs: dict) -> None:
        self.points[(round(obs["lat"], 3), round(obs["lon"], 3))] = obs

    def nearest(self, lat: float, lon: float) -> dict | None:
        best, best_d = None, self.max_nm
        for (plat, plon), obs in self.points.items():
            if abs(plat - lat) > 2.5:        # filtre rapide avant le calcul exact
                continue
            d = haversine_nm(lat, lon, plat, plon)
            if d <= best_d:
                best, best_d = obs, d
        if best is None:
            return None
        return {**best, "distance_km": round(best_d * KM_PER_NM)}

    def __len__(self) -> int:
        return len(self.points)


def weather_note(w: dict | None) -> str:
    """Complément de message d'alerte : « — mer forte (2,8 m), vent fort (25 nd) » si utile."""
    state = sea_state(w)
    if not state or state["level"] < 2:
        return ""
    return " — météo : " + ", ".join(state["causes"])
