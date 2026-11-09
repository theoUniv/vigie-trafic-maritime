"""État en mémoire de chaque navire : historique récent de positions."""
from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

MINUTE_MS = 60_000


@dataclass(frozen=True)
class Point:
    ts: int            # epoch ms
    lat: float
    lon: float
    sog: float | None
    cog: float | None
    heading: float | None
    nav_status: int

    @classmethod
    def from_msg(cls, msg: dict) -> "Point":
        ns = msg.get("nav_status")
        return cls(int(msg["ts"]), float(msg["lat"]), float(msg["lon"]), msg.get("sog"),
                   msg.get("cog"), msg.get("heading"), int(ns) if ns is not None else 15)


@dataclass
class VesselTrack:
    mmsi: int
    points: deque = field(default_factory=lambda: deque(maxlen=240))  # ~4 h à 1 pos/min

    @property
    def last(self) -> Point | None:
        return self.points[-1] if self.points else None

    @property
    def previous(self) -> Point | None:
        return self.points[-2] if len(self.points) >= 2 else None

    def add(self, p: Point) -> bool:
        """Ajoute un point ; refuse les doublons et les messages plus anciens que le dernier."""
        if self.points and p.ts <= self.points[-1].ts:
            return False
        self.points.append(p)
        return True

    def window(self, start_ms: int, end_ms: int) -> list[Point]:
        return [p for p in self.points if start_ms <= p.ts <= end_ms]

    def avg_sog(self, minutes: float, end_ms: int | None = None, offset_min: float = 0) -> float | None:
        """Vitesse moyenne sur la fenêtre [fin - offset - minutes, fin - offset]."""
        if not self.points:
            return None
        end = (end_ms if end_ms is not None else self.points[-1].ts) - int(offset_min * MINUTE_MS)
        pts = [p.sog for p in self.window(end - int(minutes * MINUTE_MS), end) if p.sog is not None]
        return sum(pts) / len(pts) if pts else None

    def stopped_duration_min(self, threshold_kn: float = 0.5) -> float:
        """Durée (min) depuis laquelle le navire est continûment sous `threshold_kn`."""
        if not self.points or self.points[-1].sog is None or self.points[-1].sog >= threshold_kn:
            return 0.0
        start = self.points[-1].ts
        for p in reversed(self.points):
            if p.sog is None or p.sog >= threshold_kn:
                break
            start = p.ts
        return (self.points[-1].ts - start) / MINUTE_MS
