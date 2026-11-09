"""Source aisstream.io (couverture mondiale, clé API gratuite : https://aisstream.io).

Le flux WebSocket est continu ; on le regroupe en instantanés d'une minute
(dernière position connue par navire) pour respecter la même cadence que Digitraffic
et éviter de saturer Kafka avec plusieurs messages par seconde et par navire.
"""
from __future__ import annotations

import asyncio
import json
import logging
import queue
import threading
import time
from collections.abc import Iterator
from datetime import datetime, timezone

import websockets

log = logging.getLogger(__name__)

URL = "wss://stream.aisstream.io/v0/stream"
POSITION_TYPES = ("PositionReport", "StandardClassBPositionReport", "ExtendedClassBPositionReport")


def _parse_time(meta: dict) -> int:
    raw = meta.get("time_utc", "")
    try:  # ex : "2024-05-01 12:34:56.123456789 +0000 UTC"
        return int(datetime.strptime(raw[:19], "%Y-%m-%d %H:%M:%S")
                   .replace(tzinfo=timezone.utc).timestamp() * 1000)
    except ValueError:
        return int(time.time() * 1000)


def parse_bboxes(spec: str) -> list[list[list[float]]]:
    boxes = []
    for part in spec.split(";"):
        lat1, lon1, lat2, lon2 = (float(x) for x in part.split(","))
        boxes.append([[lat1, lon1], [lat2, lon2]])
    return boxes


def normalize(message: dict) -> tuple[str, dict] | None:
    mtype = message.get("MessageType")
    meta = message.get("MetaData", {})
    body = message.get("Message", {}).get(mtype, {})
    mmsi = meta.get("MMSI") or body.get("UserID")
    if not mmsi:
        return None
    if mtype in POSITION_TYPES:
        lat, lon = body.get("Latitude", meta.get("latitude")), body.get("Longitude", meta.get("longitude"))
        if lat is None or lon is None or abs(lat) > 90 or abs(lon) > 180:
            return None
        sog, cog, hdg = body.get("Sog"), body.get("Cog"), body.get("TrueHeading")
        return "position", {
            "mmsi": int(mmsi), "ts": _parse_time(meta), "lat": float(lat), "lon": float(lon),
            "sog": None if sog is None or sog >= 102.3 else sog,
            "cog": None if cog is None or cog >= 360 else cog,
            "heading": None if hdg is None or hdg == 511 else hdg,
            "nav_status": body.get("NavigationalStatus", 15),
            "rot": body.get("RateOfTurn"), "source": "aisstream",
        }
    if mtype == "ShipStaticData":
        dim = body.get("Dimension") or {}
        eta = body.get("Eta") or {}
        return "static", {
            "mmsi": int(mmsi), "ts": _parse_time(meta),
            "name": (body.get("Name") or meta.get("ShipName") or "").strip() or None,
            "callsign": (body.get("CallSign") or "").strip() or None,
            "imo": body.get("ImoNumber") or None, "ship_type": body.get("Type"),
            "destination": (body.get("Destination") or "").strip() or None,
            "eta": {"month": eta.get("Month", 0), "day": eta.get("Day", 0),
                    "hour": eta.get("Hour", 24), "minute": eta.get("Minute", 60)} if eta else None,
            "draught": body.get("MaximumStaticDraught") or None,
            "length": (dim.get("A", 0) + dim.get("B", 0)) or None,
            "width": (dim.get("C", 0) + dim.get("D", 0)) or None,
            "source": "aisstream",
        }
    return None


class AisStreamSource:
    def __init__(self, api_key: str, bboxes: str, poll_interval_s: int = 60):
        if not api_key:
            raise ValueError("AISSTREAM_API_KEY est requise pour la source aisstream")
        self.api_key = api_key
        self.bboxes = parse_bboxes(bboxes)
        self.poll_interval_s = poll_interval_s
        self._queue: queue.Queue = queue.Queue(maxsize=200_000)

    async def _listen(self) -> None:
        sub = {"APIKey": self.api_key, "BoundingBoxes": self.bboxes,
               "FilterMessageTypes": [*POSITION_TYPES, "ShipStaticData"]}
        while True:
            try:
                async with websockets.connect(URL, ping_interval=20) as ws:
                    await ws.send(json.dumps(sub))
                    log.info("Connecté à aisstream.io (%d zone(s))", len(self.bboxes))
                    async for raw in ws:
                        parsed = normalize(json.loads(raw))
                        if parsed:
                            try:
                                self._queue.put_nowait(parsed)
                            except queue.Full:
                                pass
            except Exception as exc:
                log.error("Flux aisstream interrompu (%s), reconnexion dans 5 s", exc)
                await asyncio.sleep(5)

    def snapshots(self) -> Iterator[dict]:
        threading.Thread(target=lambda: asyncio.run(self._listen()), daemon=True).start()
        while True:
            deadline = time.monotonic() + self.poll_interval_s
            latest_pos: dict[int, dict] = {}
            latest_static: dict[int, dict] = {}
            while (remaining := deadline - time.monotonic()) > 0:
                try:
                    kind, msg = self._queue.get(timeout=remaining)
                except queue.Empty:
                    break
                target = latest_pos if kind == "position" else latest_static
                if msg["mmsi"] not in target or msg["ts"] >= target[msg["mmsi"]]["ts"]:
                    target[msg["mmsi"]] = msg
            yield {"positions": list(latest_pos.values()), "static": list(latest_static.values())}
