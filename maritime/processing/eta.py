"""Estimation de l'heure d'arrivée et détection des retards.

L'ETA déclarée par l'équipage (message AIS statique) est comparée à une ETA
recalculée : distance restante jusqu'au port de destination (orthodromie × facteur
de détour) divisée par la vitesse moyenne récente du navire.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from ..ais import TYPICAL_SPEED_KN
from ..geo import haversine_nm

DETOUR_FACTOR = 1.25     # les routes maritimes ne sont pas des lignes droites
ARRIVAL_RADIUS_NM = 3.0
MOVING_MIN_KN = 3.0
DELAY_WARNING_MIN = 60
DELAY_CRITICAL_MIN = 240
ALERT_MAX_DISTANCE_NM = 300   # au-delà, l'estimation reste affichée mais ne déclenche pas d'alerte

STATUS_LABELS = {
    "arrive": "Arrivé",
    "a_l_heure": "À l'heure",
    "avance": "En avance",
    "retard_probable": "Retard probable",
    "retard_important": "Retard important",
    "eta_depassee": "ETA dépassée",
    "sans_eta": "ETA non déclarée",
}


def estimate(now: datetime, lat: float, lon: float, avg_sog: float | None, category: str,
             port, eta_declared: datetime | None) -> dict | None:
    """Renvoie l'estimation d'arrivée pour un navire dont la destination est connue."""
    if port is None:
        return None
    direct = haversine_nm(lat, lon, port.lat, port.lon)
    remaining = direct * DETOUR_FACTOR
    result = {
        "dest_locode": port.locode, "dest_name": port.name, "dest_lat": port.lat, "dest_lon": port.lon,
        "remaining_nm": round(remaining, 1),
        "eta_declared": eta_declared.isoformat() if eta_declared else None,
        "eta_predicted": None, "delay_min": None, "speed_used": None, "basis": None,
    }
    if direct <= ARRIVAL_RADIUS_NM:
        result.update(status="arrive", remaining_nm=0.0)
        return _labelled(result)

    if avg_sog is not None and avg_sog >= MOVING_MIN_KN:
        speed, basis = avg_sog, "vitesse moyenne 30 min"
    else:
        speed, basis = TYPICAL_SPEED_KN.get(category, 10.0), "vitesse type (navire à l'arrêt)"
    predicted = now + timedelta(hours=remaining / speed)
    result.update(eta_predicted=predicted.isoformat(), speed_used=round(speed, 1), basis=basis)

    if eta_declared is None:
        result["status"] = "sans_eta"
        return _labelled(result)

    delay = (predicted - eta_declared).total_seconds() / 60
    result["delay_min"] = round(delay)
    if eta_declared < now:
        status = "eta_depassee"
    elif delay >= DELAY_CRITICAL_MIN:
        status = "retard_important"
    elif delay >= DELAY_WARNING_MIN:
        status = "retard_probable"
    elif delay <= -DELAY_CRITICAL_MIN:
        status = "avance"
    else:
        status = "a_l_heure"
    result["status"] = status
    return _labelled(result)


def _labelled(result: dict) -> dict:
    result["status_label"] = STATUS_LABELS[result["status"]]
    return result
