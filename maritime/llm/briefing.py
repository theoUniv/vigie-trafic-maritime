"""Bulletin de situation maritime : faits structurés -> texte rédigé par le LLM.

Logique pure (sans Kafka, base ni réseau) : le LLM ne reçoit que des faits déjà calculés par le
pipeline, ce qui limite les hallucinations, et un gabarit déterministe sert de repli.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

from ..processing.anomalies import KIND_LABELS

SEVERITY_RANK = {"critical": 0, "warning": 1, "info": 2}
LEVEL_RANK = {"congestion": 0, "ralentissement": 1, "dense": 2}
LEVEL_LABELS = {"congestion": "Congestion", "ralentissement": "Ralentissement", "dense": "Trafic dense"}

SYSTEM_PROMPT = """Tu es l'officier de quart d'un centre de surveillance du trafic maritime.
Rédige en français un bulletin de situation clair et factuel, à partir UNIQUEMENT des faits JSON
fournis. N'invente aucun navire, chiffre, lieu ni cause. Si une information manque, ne la mentionne pas.

Format (texte brut, pas de titre ni de markdown gras) :
1. Une ou deux phrases de synthèse de la situation générale (navires actifs, niveau d'activité).
2. Une ligne « Points d'attention : » suivie de 2 à 5 puces commençant par « - », du plus grave
   au moins grave (zones congestionnées, alertes critiques, retards).
3. Une ligne « À surveiller : » avec une phrase de recommandation.
Si la situation est calme, dis-le simplement. 180 mots maximum."""


def _minutes_ago(ts_ms: int | None, now_ms: int) -> int | None:
    return None if ts_ms is None else max(0, round((now_ms - ts_ms) / 60000))


def build_facts(alerts: list[dict], cells: list[dict], kpi: dict, window_min: int,
                now_ms: int, max_alerts: int = 8, max_zones: int = 6) -> dict:
    """Résume les alertes et zones de la période en un petit document JSON pour le LLM."""
    since = now_ms - window_min * 60000
    recent = [a for a in alerts if (a.get("ts") or 0) >= since]

    by_kind = Counter(a.get("kind") for a in recent)
    notable = sorted(recent, key=lambda a: (SEVERITY_RANK.get(a.get("severity"), 3), -(a.get("ts") or 0)))
    zones = sorted((c for c in cells if c.get("level") in LEVEL_RANK),
                   key=lambda c: (LEVEL_RANK[c["level"]], -(c.get("score") or 0)))

    return {
        "periode_minutes": window_min,
        "heure_utc": datetime.fromtimestamp(now_ms / 1000, timezone.utc).strftime("%Y-%m-%d %H:%M"),
        "flotte": {
            "navires_actifs": kpi.get("active"),
            "en_route": kpi.get("moving"),
            "au_mouillage": kpi.get("anchored"),
            "a_quai": kpi.get("moored"),
            "en_retard": kpi.get("delayed"),
            "vitesse_moyenne_nd": float(kpi["mean_speed"]) if kpi.get("mean_speed") is not None else None,
        },
        "alertes_total": len(recent),
        "alertes_par_type": [
            {"type": KIND_LABELS.get(k, k), "nombre": n} for k, n in by_kind.most_common()
        ],
        "alertes_notables": [
            {
                "type": a.get("label") or KIND_LABELS.get(a.get("kind"), a.get("kind")),
                "gravite": a.get("severity"),
                "navire": a.get("vessel_name") or (f"MMSI {a['mmsi']}" if a.get("mmsi") else None),
                "mmsi": a.get("mmsi"),
                "message": a.get("message"),
                "il_y_a_min": _minutes_ago(a.get("ts"), now_ms),
            }
            for a in notable[:max_alerts]
        ],
        "zones": [
            {
                "niveau": LEVEL_LABELS[c["level"]],
                "lieu": c.get("place") or f"{(c['lat_min'] + c['lat_max']) / 2:.2f}N "
                                         f"{(c['lon_min'] + c['lon_max']) / 2:.2f}E",
                "navires": c.get("n_vessels"),
                "cargos_en_attente": c.get("n_waiting"),
                "vitesse_moyenne_nd": round(c["mean_sog"], 1) if c.get("mean_sog") is not None else None,
                "vitesse_normale_nd": round(c["baseline_sog"], 1) if c.get("baseline_sog") else None,
            }
            for c in zones[:max_zones]
        ],
    }


def briefing_messages(facts: dict) -> list[dict]:
    import json
    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": "Faits de la période :\n" + json.dumps(facts, ensure_ascii=False, indent=1)},
    ]


def clean_output(text: str) -> str:
    """Retire les éventuelles balises de code et le gras markdown produits par le modèle."""
    text = (text or "").strip()
    if text.startswith("```"):
        text = text.strip("`").split("\n", 1)[-1]
    return text.replace("**", "").strip()


def template_bulletin(facts: dict) -> str:
    """Bulletin déterministe utilisé quand le LLM est désactivé ou indisponible."""
    f = facts["flotte"]
    lines = [f"Situation à {facts['heure_utc']} UTC : {f.get('navires_actifs') or 0} navires actifs, "
             f"dont {f.get('en_route') or 0} en route et {f.get('au_mouillage') or 0} au mouillage. "
             f"{facts['alertes_total']} alerte(s) sur les {facts['periode_minutes']} dernières minutes."]
    points = [f"- {z['niveau']} : {z['lieu']} ({z['navires']} navires, {z['cargos_en_attente']} en attente)"
              for z in facts["zones"] if z["niveau"] != "Trafic dense"][:3]
    points += [f"- {a['type']} : {a['navire'] or 'navire inconnu'} — {a['message']}"
               for a in facts["alertes_notables"] if a["gravite"] in ("critical", "warning")][:5 - len(points)]
    if points:
        lines.append("Points d'attention :")
        lines.extend(points)
    else:
        lines.append("Points d'attention : aucun, la situation est calme.")
    if f.get("en_retard"):
        lines.append(f"À surveiller : {f['en_retard']} navire(s) en retard probable par rapport à leur ETA déclarée.")
    return "\n".join(lines)
