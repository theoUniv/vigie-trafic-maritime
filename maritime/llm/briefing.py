"""Bulletin de situation maritime : faits structurés -> texte rédigé par le LLM.

Logique pure (sans Kafka, base ni réseau) : le LLM ne reçoit que des faits déjà calculés par le
pipeline, ce qui limite les hallucinations, et un gabarit déterministe sert de repli.
"""
from __future__ import annotations

import re
from collections import Counter
from datetime import datetime, timezone

from ..processing.anomalies import KIND_LABELS

SEVERITY_RANK = {"critical": 0, "warning": 1, "info": 2}
LEVEL_RANK = {"congestion": 0, "ralentissement": 1, "dense": 2}
LEVEL_LABELS = {"congestion": "Congestion", "ralentissement": "Ralentissement", "dense": "Trafic dense"}

SYSTEM_PROMPT = """Tu es l'officier de quart d'un centre de surveillance du trafic maritime.
Rédige en français un bulletin de situation clair et factuel, à partir UNIQUEMENT des faits JSON
fournis. N'invente aucun navire, chiffre, lieu ni cause. Si une information manque, ne la mentionne pas.

Sens des champs (ne les confonds pas) :
- flotte.navires_en_retard : navires dont l'arrivée estimée dépasse l'ETA déclarée par l'équipage.
- alertes_total : nombre d'ALERTES détectées sur la période (ce n'est pas un nombre de navires).
- zones[].cargos_en_attente : cargos à l'arrêt ou au mouillage dans la zone.
- zones[].vitesse_habituelle_nd : vitesse normale apprise pour la zone, à comparer à vitesse_moyenne_nd.

Format, en texte brut, sans numérotation, sans titre ni gras :
- Commence directement par une ou deux phrases de synthèse (nombre de navires actifs, niveau d'activité).
- Puis la ligne « Points d'attention : » suivie de 2 à 5 puces commençant par « - », du plus grave au
  moins grave (zones congestionnées avec leur lieu, alertes critiques, retards notables avec le nom du navire).
- Termine par une ligne commençant par « À surveiller : » avec une phrase de recommandation.

Exemple de forme (chiffres fictifs, à remplacer par les vrais faits) :
Activité soutenue avec 650 navires actifs, dont 200 en route.
Points d'attention :
- Congestion près de Kotka : 5 cargos en attente, vitesse moyenne 0,2 nd.
- Arrêt en mer inattendu du navire NORDIC STAR.
À surveiller : la file d'attente devant Kotka.

Si la situation est calme, dis-le simplement. 150 mots maximum."""


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
            "navires_en_retard": kpi.get("delayed"),
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
        "zones": _merge_zones(zones)[:max_zones],
    }


def _merge_zones(cells: list[dict]) -> list[dict]:
    """Une entrée par lieu : un port couvre souvent plusieurs cellules de la grille de 5 km,
    que le LLM répéterait sinon. Les cellules déjà triées gardent leur ordre de gravité."""
    merged: dict[tuple, dict] = {}
    for c in cells:
        place = c.get("place") or (f"{(c['lat_min'] + c['lat_max']) / 2:.2f}N "
                                   f"{(c['lon_min'] + c['lon_max']) / 2:.2f}E")
        key = (place, c["level"])
        z = merged.get(key)
        if z is None:
            merged[key] = z = {"niveau": LEVEL_LABELS[c["level"]], "lieu": place, "navires": 0,
                               "cargos_en_attente": 0, "_sog": [], "_base": []}
        z["navires"] += c.get("n_vessels") or 0
        z["cargos_en_attente"] += c.get("n_waiting") or 0
        if c.get("mean_sog") is not None:
            z["_sog"].append((c["mean_sog"], c.get("n_vessels") or 1))
        if c.get("baseline_sog"):
            z["_base"].append(c["baseline_sog"])
    out = []
    for z in merged.values():
        sog, base = z.pop("_sog"), z.pop("_base")
        weight = sum(w for _, w in sog)
        z["vitesse_moyenne_nd"] = round(sum(v * w for v, w in sog) / weight, 1) if weight else None
        z["vitesse_habituelle_nd"] = round(sum(base) / len(base), 1) if base else None
        out.append(z)
    return out


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
    text = text.replace("**", "")
    # Numérotation de sections (« 1. Synthèse : ») que les petits modèles ajoutent malgré la consigne
    text = re.sub(r"(?m)^\s*\d+[.)]\s+", "", text)
    return text.strip()


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
    if f.get("navires_en_retard"):
        lines.append(f"À surveiller : {f['navires_en_retard']} navire(s) en retard probable par rapport à leur ETA déclarée.")
    return "\n".join(lines)
