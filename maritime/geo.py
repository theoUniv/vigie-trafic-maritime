"""Outils géographiques : distances, caps, grille de cellules."""
from __future__ import annotations

import math

EARTH_RADIUS_NM = 3440.065  # rayon terrestre en milles nautiques
KM_PER_DEG_LAT = 111.32


def haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance orthodromique en milles nautiques."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_NM * math.asin(min(1.0, math.sqrt(a)))


def bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Cap initial (0-360°) du point 1 vers le point 2."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    x = math.sin(dl) * math.cos(p2)
    y = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return (math.degrees(math.atan2(x, y)) + 360) % 360


def cell_of(lat: float, lon: float, cell_km: float) -> tuple[int, int]:
    """Indice (ligne, colonne) d'une cellule ~carrée de `cell_km` de côté.

    La largeur en longitude d'une ligne dépend de la latitude de son centre,
    afin que les cellules restent à peu près carrées quelle que soit la latitude.
    """
    dlat = cell_km / KM_PER_DEG_LAT
    i = math.floor((lat + 90) / dlat)
    dlon = _row_dlon(i, dlat)
    j = math.floor((lon + 180) / dlon)
    return i, j


def cell_bounds(i: int, j: int, cell_km: float) -> tuple[float, float, float, float]:
    """(lat_min, lon_min, lat_max, lon_max) d'une cellule."""
    dlat = cell_km / KM_PER_DEG_LAT
    dlon = _row_dlon(i, dlat)
    lat_min = i * dlat - 90
    lon_min = j * dlon - 180
    return lat_min, lon_min, lat_min + dlat, lon_min + dlon


def _row_dlon(i: int, dlat: float) -> float:
    lat_center = (i + 0.5) * dlat - 90
    return dlat / max(math.cos(math.radians(lat_center)), 0.05)
