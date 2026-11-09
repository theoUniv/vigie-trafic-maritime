"""API REST + interface web.

    uvicorn maritime.api.app:app --host 0.0.0.0 --port 8000
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from ..ais import CATEGORIES, COMMERCIAL_CATEGORIES, nav_status_label, ship_type_label
from ..config import settings
from ..processing.anomalies import KIND_LABELS

WEB_DIR = Path(os.environ.get("WEB_DIR", Path(__file__).resolve().parents[2] / "web"))
pool: ConnectionPool | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global pool
    pool = ConnectionPool(settings.database_url, min_size=1, max_size=8,
                          kwargs={"row_factory": dict_row, "autocommit": True}, open=True)
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
    return v


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
               baseline_sog, score, level, place
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


# --- Interface web ---------------------------------------------------------------
if WEB_DIR.exists():
    app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")

    @app.get("/", include_in_schema=False)
    def index():
        return FileResponse(WEB_DIR / "index.html")
