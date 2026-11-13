"""API REST + interface web.

    uvicorn maritime.api.app:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import json
import os
import threading
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ..ais import CATEGORIES, COMMERCIAL_CATEGORIES, nav_status_label, ship_type_label
from ..config import settings
from ..llm.assistant import Assistant
from ..llm.client import LLMError, OllamaClient
from ..processing.anomalies import KIND_LABELS
from ..processing.weather import sea_state

WEB_DIR = Path(os.environ.get("WEB_DIR", Path(__file__).resolve().parents[2] / "web"))
pool: ConnectionPool | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global pool
    pool = ConnectionPool(settings.database_url, min_size=1, max_size=8,
                          kwargs={"row_factory": dict_row, "autocommit": True}, open=True)
    # Précharge le modèle en arrière-plan pour que la première question ne paie pas le chargement
    threading.Thread(target=llm.warmup, daemon=True).start()
    yield
    pool.close()


app = FastAPI(title="Vigie — trafic maritime temps réel", version="1.0.0", lifespan=lifespan)


def q(sql: str, params=None) -> list[dict]:
    with pool.connection() as conn:
        return conn.execute(sql, params or {}).fetchall()


ACTIVE = f"vs.ts > now() - interval '{int(os.environ.get('MAP_WINDOW_MIN', 30))} minutes'"


@app.get("/api/health")
def health():
    row = q("SELECT max(ts) AS last_position, count(*) AS vessels FROM vessel_state")[0]
    return {"status": "ok", **row}


@app.get("/api/meta")
def meta():
    """Valeurs disponibles pour les filtres (catégories, pays présents)."""
    countries = q(f"""
        SELECT v.country_code AS code, v.country AS name, count(*) AS n
        FROM vessel_state vs JOIN vessels v USING (mmsi)
        WHERE {ACTIVE} GROUP BY 1, 2 ORDER BY n DESC""")
    categories = q(f"""
        SELECT v.category AS code, count(*) AS n
        FROM vessel_state vs JOIN vessels v USING (mmsi)
        WHERE {ACTIVE} GROUP BY 1 ORDER BY n DESC""")
    for c in categories:
        c["label"] = CATEGORIES.get(c["code"], c["code"])
    return {"countries": countries, "categories": categories, "anomaly_kinds": KIND_LABELS,
            "source": settings.ais_source}


def _filters(q_text: str | None, category: list[str] | None, country: list[str] | None,
             status: str | None, commercial: bool, bbox: str | None):
    where, params = [ACTIVE], {}
    if q_text:
        params["q"] = f"%{q_text.strip().lower()}%"
        params["qn"] = q_text.strip()
        where.append("(lower(v.name) LIKE %(q)s OR lower(v.callsign) LIKE %(q)s "
                     "OR vs.mmsi::text = %(qn)s OR v.imo::text = %(qn)s "
                     "OR lower(v.destination) LIKE %(q)s)")
    if category:
        params["cat"] = category
        where.append("v.category = ANY(%(cat)s)")
    if commercial:
        params["com"] = list(COMMERCIAL_CATEGORIES)
        where.append("v.category = ANY(%(com)s)")
    if country:
        params["cc"] = country
        where.append("v.country_code = ANY(%(cc)s)")
    if status == "moving":
        where.append("vs.sog >= 1")
    elif status == "stopped":
        where.append("(vs.sog < 1 OR vs.sog IS NULL)")
    elif status == "delayed":
        where.append("vs.eta_estimate->>'status' IN ('retard_probable', 'retard_important')")
    if bbox:
        try:
            s, w, n, e = (float(x) for x in bbox.split(","))
        except ValueError:
            raise HTTPException(400, "bbox attendu : sud,ouest,nord,est")
        params.update(s=s, w=w, n=n, e=e)
        where.append("vs.lat BETWEEN %(s)s AND %(n)s AND vs.lon BETWEEN %(w)s AND %(e)s")
    return " AND ".join(where), params


@app.get("/api/vessels")
def vessels(q_text: str | None = Query(None, alias="q"),
            category: list[str] | None = Query(None),
            country: list[str] | None = Query(None),
            status: str | None = None,
            commercial: bool = False,
            bbox: str | None = None,
            limit: int = Query(20000, le=50000)):
    """Dernière position des navires actifs, filtrable par nom, type, pays, état et zone."""
    where, params = _filters(q_text, category, country, status, commercial, bbox)
    params["limit"] = limit
    rows = q(f"""
        SELECT vs.mmsi, v.name, v.category, v.country_code, vs.lat, vs.lon, vs.sog, vs.cog,
               vs.heading, vs.nav_status, extract(epoch FROM vs.ts)::bigint AS ts,
               vs.eta_estimate->>'status' AS eta_status
        FROM vessel_state vs JOIN vessels v USING (mmsi)
        WHERE {where} ORDER BY vs.mmsi LIMIT %(limit)s""", params)
    return {"count": len(rows), "vessels": rows}


@app.get("/api/search")
def search(q_text: str = Query(..., alias="q", min_length=2), limit: int = 15):
    where, params = _filters(q_text, None, None, None, False, None)
    params["limit"] = limit
    return q(f"""
        SELECT vs.mmsi, v.name, v.category, v.country_code, v.country, v.destination, vs.lat, vs.lon
        FROM vessel_state vs JOIN vessels v USING (mmsi)
        WHERE {where} ORDER BY v.name NULLS LAST LIMIT %(limit)s""", params)


@app.get("/api/vessels/{mmsi}")
def vessel(mmsi: int):
    rows = q("""
        SELECT v.*, vs.ts AS position_ts, vs.lat, vs.lon, vs.sog, vs.cog, vs.heading,
               vs.nav_status, vs.avg_sog_30m, vs.eta_estimate
        FROM vessels v LEFT JOIN vessel_state vs USING (mmsi) WHERE v.mmsi = %(m)s""", {"m": mmsi})
    if not rows:
        raise HTTPException(404, "Navire inconnu")
    v = rows[0]
    v["type_label"] = ship_type_label(v["ship_type"])
    v["category_label"] = CATEGORIES.get(v["category"], v["category"])
    v["nav_status_label"] = nav_status_label(v["nav_status"])
    v["anomalies"] = q("""
        SELECT ts, kind, severity, message FROM anomalies
        WHERE mmsi = %(m)s ORDER BY ts DESC LIMIT 20""", {"m": mmsi})
    for a in v["anomalies"]:
        a["label"] = KIND_LABELS.get(a["kind"], a["kind"])
    v["weather"] = nearest_weather(v["lat"], v["lon"]) if v["lat"] is not None else None
    return v


WEATHER_COLS = ("lat, lon, ts, wind_kn, wind_dir, gust_kn, visibility_m, weather_code, wave_m, "
                "wave_dir, wave_period_s, sea_temp_c")


def nearest_weather(lat: float, lon: float, max_deg: float = 1.2) -> dict | None:
    """Observation Open-Meteo la plus proche (distance approchée, la grille est au demi-degré)."""
    rows = q(f"""
        SELECT {WEATHER_COLS} FROM weather_current
        WHERE ts > now() - interval '6 hours'
          AND lat BETWEEN %(lat)s - %(d)s AND %(lat)s + %(d)s
          AND lon BETWEEN %(lon)s - 2 * %(d)s AND %(lon)s + 2 * %(d)s
        ORDER BY power(lat - %(lat)s, 2) + power((lon - %(lon)s) * cos(radians(%(lat)s)), 2)
        LIMIT 1""", {"lat": lat, "lon": lon, "d": max_deg})
    if not rows:
        return None
    return {**rows[0], "state": sea_state(rows[0])}


@app.get("/api/weather")
def weather():
    """Météo marine Open-Meteo sur la grille de points en mer (vent, rafales, vagues, visibilité)."""
    rows = q(f"SELECT {WEATHER_COLS} FROM weather_current WHERE ts > now() - interval '6 hours'")
    for r in rows:
        r["state"] = sea_state(r)
    return {"count": len(rows), "points": rows,
            "source": "Open-Meteo (CC BY 4.0)"}


@app.get("/api/vessels/{mmsi}/track")
def track(mmsi: int, hours: float = Query(6, gt=0, le=48)):
    rows = q("""
        SELECT extract(epoch FROM ts)::bigint AS ts, lat, lon, sog, cog, nav_status
        FROM positions WHERE mmsi = %(m)s AND ts > now() - make_interval(secs => %(s)s)
        ORDER BY ts""", {"m": mmsi, "s": hours * 3600})
    return {"mmsi": mmsi, "points": rows}


@app.get("/api/congestion")
def congestion(min_level: str = "dense"):
    levels = {"normal": ["normal", "dense", "ralentissement", "congestion"],
              "dense": ["dense", "ralentissement", "congestion"],
              "ralentissement": ["ralentissement", "congestion"],
              "congestion": ["congestion"]}.get(min_level, ["congestion"])
    return q("""
        SELECT cell_id, ts, lat_min, lon_min, lat_max, lon_max, n_vessels, n_waiting, mean_sog,
               baseline_sog, score, level, place, weather
        FROM congestion_current WHERE level = ANY(%(l)s) ORDER BY score DESC""", {"l": levels})


@app.get("/api/anomalies")
def anomalies(hours: float = Query(6, gt=0, le=168), kind: list[str] | None = Query(None),
              limit: int = Query(300, le=2000)):
    params = {"s": hours * 3600, "limit": limit}
    extra = ""
    if kind:
        params["k"] = kind
        extra = "AND a.kind = ANY(%(k)s)"
    rows = q(f"""
        SELECT a.id, a.ts, a.mmsi, a.kind, a.severity, a.lat, a.lon, a.message, a.details,
               v.name AS vessel_name, v.country_code
        FROM anomalies a LEFT JOIN vessels v USING (mmsi)
        WHERE a.ts > now() - make_interval(secs => %(s)s) {extra}
        ORDER BY a.ts DESC LIMIT %(limit)s""", params)
    for a in rows:
        a["label"] = KIND_LABELS.get(a["kind"], a["kind"])
    return rows


@app.get("/api/stats")
def stats():
    kpi = q(f"""
        SELECT count(*) AS active,
               count(*) FILTER (WHERE vs.sog >= 1) AS moving,
               count(*) FILTER (WHERE vs.nav_status = 1) AS anchored,
               count(*) FILTER (WHERE vs.nav_status = 5) AS moored,
               count(*) FILTER (WHERE vs.eta_estimate->>'status'
                                IN ('retard_probable','retard_important')) AS delayed,
               round(avg(vs.sog) FILTER (WHERE vs.sog >= 1)::numeric, 1) AS mean_speed,
               max(vs.ts) AS last_position
        FROM vessel_state vs WHERE {ACTIVE}""")[0]
    kpi["alerts_1h"] = q("SELECT count(*) AS n FROM anomalies WHERE ts > now() - interval '1 hour'")[0]["n"]
    kpi["congested_zones"] = q(
        "SELECT count(*) AS n FROM congestion_current WHERE level IN ('congestion','ralentissement')")[0]["n"]
    by_category = q(f"""
        SELECT v.category AS code, count(*) AS n, round(avg(vs.sog)::numeric, 1) AS mean_sog
        FROM vessel_state vs JOIN vessels v USING (mmsi) WHERE {ACTIVE}
        GROUP BY 1 ORDER BY n DESC""")
    for c in by_category:
        c["label"] = CATEGORIES.get(c["code"], c["code"])
    throughput = q("""
        SELECT extract(epoch FROM ts)::bigint AS ts, positions, alerts, lag_s
        FROM ingest_stats WHERE ts > now() - interval '3 hours' ORDER BY ts""")
    by_kind = q("""
        SELECT kind, count(*) AS n FROM anomalies WHERE ts > now() - interval '24 hours'
        GROUP BY 1 ORDER BY n DESC""")
    for k in by_kind:
        k["label"] = KIND_LABELS.get(k["kind"], k["kind"])
    return {"kpi": kpi, "by_category": by_category, "throughput": throughput, "anomalies_24h": by_kind}


# --- IA : bulletins et assistant (LLM open source local via Ollama) ------------------
llm = OllamaClient(settings.llm_url, settings.llm_model, settings.llm_timeout_s)


@app.get("/api/llm/status")
def llm_status():
    """État du LLM : activé, modèle, serveur joignable et modèle téléchargé."""
    return {"enabled": llm.enabled, "model": llm.model, "available": llm.available()}


@app.get("/api/briefings")
def briefings(limit: int = Query(10, ge=1, le=100)):
    """Derniers bulletins de situation (le plus récent en premier)."""
    return q("""
        SELECT id, ts, window_min, text, generator, model, duration_s
        FROM briefings ORDER BY ts DESC LIMIT %(l)s""", {"l": limit})


def _num(v, nd=1):
    return None if v is None else round(float(v), nd)


# Les outils renvoient des champs courts et explicites en français : un petit modèle lit moins de
# texte (plus rapide sur CPU) et confond moins les grandeurs.
def tool_search_vessels(query: str | None = None, category: str | None = None,
                        country: str | None = None, status: str | None = None,
                        limit: int = 10) -> dict:
    if category and category not in CATEGORIES:
        category = None
    where, params = _filters(query or None, [category] if category else None,
                             [country.upper()] if country else None, status, False, None)
    params["limit"] = max(1, min(int(limit), 20))
    total = q(f"SELECT count(*) AS n FROM vessel_state vs JOIN vessels v USING (mmsi) WHERE {where}",
              params)[0]["n"]
    rows = q(f"""
        SELECT vs.mmsi, v.name, v.category, v.country, v.destination, vs.sog,
               vs.eta_estimate->>'status' AS eta_status
        FROM vessel_state vs JOIN vessels v USING (mmsi)
        WHERE {where} ORDER BY vs.sog DESC NULLS LAST LIMIT %(limit)s""", params)
    return {"nombre_total": total, "navires": [
        {"nom": r["name"], "mmsi": r["mmsi"], "type": CATEGORIES.get(r["category"], r["category"]),
         "pavillon": r["country"], "destination": r["destination"], "vitesse_nd": _num(r["sog"]),
         "statut_eta": r["eta_status"]} for r in rows]}


def tool_vessel_details(mmsi: int) -> dict:
    try:
        v = vessel(int(mmsi))
    except HTTPException:
        return {"erreur": f"aucun navire avec le MMSI {mmsi}"}
    est = v.get("eta_estimate") or {}
    return {
        "nom": v["name"], "mmsi": v["mmsi"], "imo": v["imo"], "type": v["type_label"],
        "pavillon": v["country"], "longueur_m": v["length"], "tirant_eau_m": v["draught"],
        "etat_ais": v["nav_status_label"], "vitesse_nd": _num(v["sog"]),
        "vitesse_moy_30min_nd": _num(v["avg_sog_30m"]), "cap": _num(v["cog"], 0),
        "position": {"lat": _num(v["lat"], 3), "lon": _num(v["lon"], 3)} if v["lat"] is not None else None,
        "derniere_position": v["position_ts"], "destination": v["destination"],
        "eta_declaree": v["eta"], "eta_estimee": est or None,
        "meteo_sur_zone": _meteo((v.get("weather") or {}).get("state")),
        "anomalies_recentes": [{"date": a["ts"], "type": a["label"], "message": a["message"]}
                               for a in v["anomalies"][:5]],
    }


ZONE_LABELS = {"congestion": "congestion", "ralentissement": "ralentissement", "dense": "trafic dense"}


def tool_congestion_zones(min_level: str = "ralentissement") -> dict:
    zones = congestion(min_level)
    return {"nombre_zones": len(zones), "zones": [
        {"niveau": ZONE_LABELS.get(z["level"], z["level"]), "lieu": z["place"] or "en mer",
         "navires_dans_la_zone": z["n_vessels"], "cargos_en_attente": z["n_waiting"],
         "vitesse_moyenne_nd": _num(z["mean_sog"]), "vitesse_habituelle_nd": _num(z["baseline_sog"]),
         "meteo": _meteo(z.get("weather"))}
        for z in zones[:8]]}


def _meteo(w: dict | None) -> str | None:
    """« difficile : vent 25,0 nd, vagues 2,6 m (vent fort) » : une ligne lisible pour le LLM."""
    if not w or "label" not in w:
        return None
    causes = f" ({', '.join(w['causes'])})" if w.get("causes") else ""
    return f"mer {w['label']} : {w['summary']}{causes}"



def tool_recent_alerts(hours: float = 2, kind: str | None = None, limit: int = 15) -> dict:
    rows = anomalies(max(0.1, min(float(hours), 48)), [kind] if kind else None,
                     max(1, min(int(limit), 20)))
    return {"alertes": [{"date": a["ts"], "type": a["label"], "gravite": a["severity"],
                         "navire": a["vessel_name"], "mmsi": a["mmsi"], "message": a["message"]}
                        for a in rows]}


def tool_traffic_stats() -> dict:
    s = stats()
    k = s["kpi"]
    return {
        "navires_actifs": k["active"], "en_route": k["moving"], "au_mouillage": k["anchored"],
        "a_quai": k["moored"], "navires_en_retard": k["delayed"],
        "vitesse_moyenne_en_route_nd": _num(k["mean_speed"]), "alertes_derniere_heure": k["alerts_1h"],
        "zones_congestion_ou_ralentissement": k["congested_zones"],
        "navires_par_type": {c["label"]: c["n"] for c in s["by_category"]},
        "alertes_24h_par_type": {a["label"]: a["n"] for a in s["anomalies_24h"]},
    }


def tool_sea_conditions() -> dict:
    """Synthèse de la météo marine : répartition par état de la mer et points les plus durs."""
    pts = weather()["points"]
    if not pts:
        return {"info": "pas encore de données météo (collecte toutes les 90 min)"}
    from collections import Counter
    counts = Counter(p["state"]["label"] for p in pts)
    worst = sorted(pts, key=lambda p: (-p["state"]["level"], -(p["wind_kn"] or 0)))[:6]
    return {"points_de_mesure": len(pts),
            "repartition_etat_de_la_mer": dict(counts),
            "vent_max_nd": max((p["wind_kn"] or 0) for p in pts),
            "vagues_max_m": max((p["wave_m"] or 0) for p in pts),
            "points_les_plus_durs": [{"lat": p["lat"], "lon": p["lon"], "meteo": _meteo(p["state"])}
                                     for p in worst]}


def tool_latest_briefing() -> dict:
    rows = briefings(1)
    return {"date": rows[0]["ts"], "texte": rows[0]["text"]} if rows else {"info": "aucun bulletin pour le moment"}


assistant = Assistant(llm, {
    "search_vessels": tool_search_vessels,
    "vessel_details": tool_vessel_details,
    "congestion_zones": tool_congestion_zones,
    "recent_alerts": tool_recent_alerts,
    "traffic_stats": tool_traffic_stats,
    "latest_briefing": tool_latest_briefing,
    "sea_conditions": tool_sea_conditions,
})


class ChatTurn(BaseModel):
    role: str
    content: str = Field(max_length=4000)


class AskBody(BaseModel):
    question: str = Field(min_length=2, max_length=500)
    history: list[ChatTurn] = Field(default_factory=list, max_length=20)


@app.post("/api/ask")
def ask(body: AskBody):
    """Question en langage naturel : le LLM interroge la base via des outils et répond en français.

    Réponse diffusée en NDJSON (une ligne JSON par événement) : `tool` (outil appelé), `token`
    (texte au fil de l'eau), `reset` (texte à effacer), `done` (réponse finale) ou `error`.
    """
    if not llm.available():
        raise HTTPException(503, "Assistant indisponible : LLM non démarré ou modèle en cours de téléchargement")
    history = [t.model_dump() for t in body.history]

    def events():
        try:
            for ev in assistant.ask_stream(body.question, history):
                yield json.dumps(ev, ensure_ascii=False, default=str) + "\n"
        except LLMError as exc:
            yield json.dumps({"type": "error", "message": f"Erreur du LLM : {exc}"}, ensure_ascii=False) + "\n"

    return StreamingResponse(events(), media_type="application/x-ndjson",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# --- Interface web ---------------------------------------------------------------
if WEB_DIR.exists():
    app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(WEB_DIR / "index.html")
