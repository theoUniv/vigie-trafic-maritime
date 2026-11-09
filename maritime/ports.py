"""Gazetteer des ports (UN/LOCODE) et résolution de la destination AIS déclarée.

Source : Digitraffic Port Call API (open data, ~12 000 ports géolocalisés dans le monde),
mise en cache locale. Une petite liste de secours est embarquée si l'API est injoignable.
"""
from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
from dataclasses import dataclass

import requests

from .geo import haversine_nm

log = logging.getLogger(__name__)

PORTS_URL = "https://meri.digitraffic.fi/api/port-call/v1/ports"


@dataclass(frozen=True)
class Port:
    locode: str
    name: str
    country: str
    lat: float
    lon: float


_FALLBACK = [
    ("FIHEL", "Helsinki", "Finland", 60.1567, 24.9550), ("FITKU", "Turku", "Finland", 60.4353, 22.2189),
    ("FIKTK", "Kotka", "Finland", 60.4633, 26.9458), ("FIRAU", "Rauma", "Finland", 61.1283, 21.4650),
    ("FIHKO", "Hanko", "Finland", 59.8236, 22.9681), ("FIOUL", "Oulu", "Finland", 65.0100, 25.4300),
    ("FIKOK", "Kokkola", "Finland", 63.8600, 23.0400), ("FIMHQ", "Mariehamn", "Finland", 60.0950, 19.9300),
    ("FIPRV", "Porvoo", "Finland", 60.3000, 25.5500), ("FINLI", "Naantali", "Finland", 60.4600, 22.0300),
    ("SESTO", "Stockholm", "Sweden", 59.3250, 18.0900), ("SEGOT", "Göteborg", "Sweden", 57.6900, 11.9000),
    ("SELLA", "Luleå", "Sweden", 65.5800, 22.1600), ("SEKPS", "Kapellskär", "Sweden", 59.7200, 19.0700),
    ("EETLL", "Tallinn", "Estonia", 59.4450, 24.7650), ("EEMUG", "Muuga", "Estonia", 59.4950, 24.9600),
    ("RULED", "Saint-Pétersbourg", "Russia", 59.8800, 30.2100), ("RUULU", "Ust-Luga", "Russia", 59.6800, 28.4000),
    ("RUPRI", "Primorsk", "Russia", 60.3500, 28.6800), ("RUVYS", "Vysotsk", "Russia", 60.6200, 28.5700),
    ("LVRIX", "Riga", "Latvia", 57.0000, 24.0900), ("LTKLJ", "Klaipeda", "Lithuania", 55.7000, 21.1300),
    ("PLGDN", "Gdańsk", "Poland", 54.4000, 18.6600), ("PLGDY", "Gdynia", "Poland", 54.5300, 18.5500),
    ("DEROS", "Rostock", "Germany", 54.1500, 12.1000), ("DEHAM", "Hamburg", "Germany", 53.5400, 9.9700),
    ("DETRV", "Travemünde", "Germany", 53.9600, 10.8700), ("DKCPH", "Copenhague", "Denmark", 55.7000, 12.6000),
    ("NLRTM", "Rotterdam", "Netherlands", 51.9500, 4.1400), ("BEANR", "Anvers", "Belgium", 51.2800, 4.3500),
    ("FRLEH", "Le Havre", "France", 49.4800, 0.1100), ("FRMRS", "Marseille", "France", 43.3300, 5.3500),
    ("GBFXT", "Felixstowe", "United Kingdom", 51.9500, 1.3200), ("ESALG", "Algésiras", "Spain", 36.1300, -5.4300),
    ("SGSIN", "Singapour", "Singapore", 1.2600, 103.8400), ("CNSHA", "Shanghai", "China", 31.3600, 121.6000),
]

# Alias fréquents saisis par les équipages (champ libre "destination")
_ALIASES = {
    "STPETERSBURG": "RULED", "SPB": "RULED", "STPETERBURG": "RULED", "PETERSBURG": "RULED",
    "USTLUGA": "RUULU", "HKI": "FIHEL", "HELSINGFORS": "FIHEL", "ABO": "FITKU", "GOTEBORG": "SEGOT",
    "GOTHENBURG": "SEGOT", "COPENHAGEN": "DKCPH", "KOBENHAVN": "DKCPH", "ANTWERP": "BEANR",
    "ANTWERPEN": "BEANR", "GDANSK": "PLGDN", "KLAIPEDA": "LTKLJ", "TALLIN": "EETLL",
}


def normalize(text: str) -> str:
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    return re.sub(r"[^A-Z0-9]", "", text.upper())


class PortIndex:
    def __init__(self, ports: list[Port]):
        self.by_locode: dict[str, Port] = {}
        self.by_name: dict[str, Port] = {}
        for p in ports:
            self.by_locode[p.locode] = p
            # Le premier port d'un nom l'emporte (la liste Digitraffic place la Finlande en tête)
            self.by_name.setdefault(normalize(p.name), p)
        self.ports = ports
        # Index spatial grossier (cases de 1°) pour la recherche du port le plus proche
        self._grid: dict[tuple[int, int], list[Port]] = {}
        for p in ports:
            self._grid.setdefault((int(p.lat // 1), int(p.lon // 1)), []).append(p)

    def __len__(self) -> int:
        return len(self.ports)

    def resolve(self, destination: str | None) -> Port | None:
        """Résout un champ destination AIS libre ("FI HEL", "HELSINKI", "RU ULU>FIKTK"...)."""
        if not destination:
            return None
        raw = destination.upper()
        if "<>" in raw:   # ligne régulière aller-retour (ferry) : pas de destination unique
            return None
        # Format "ORIGINE>DESTINATION" : on garde la partie après le dernier ">"
        if ">" in raw:
            raw = raw.rsplit(">", 1)[1]
        key = normalize(raw)
        if len(key) < 3:
            return None
        if key in _ALIASES and _ALIASES[key] in self.by_locode:
            return self.by_locode[_ALIASES[key]]
        if len(key) == 5 and key in self.by_locode:
            return self.by_locode[key]
        if key in self.by_name:
            return self.by_name[key]
        # Préfixe LOCODE suivi d'un texte : "FIHELVUOSAARI"
        if len(key) > 5 and key[:5] in self.by_locode:
            return self.by_locode[key[:5]]
        # Code pays + nom : "FI/HANKO", "SE STOCKHOLM"
        if len(key) > 4 and key[2:] in self.by_name:
            return self.by_name[key[2:]]
        # Dernier mot significatif : "PORT OF TALLINN", "ANCH. MUUGA"
        for token in reversed(re.split(r"[^A-Z0-9]+", raw)):
            token = normalize(token)
            if len(token) >= 4 and (token in self.by_name or token in _ALIASES):
                return self.by_locode.get(_ALIASES.get(token, ""), self.by_name.get(token))
        return None

    def nearest(self, lat: float, lon: float, max_nm: float) -> tuple[Port, float] | None:
        """Port le plus proche à moins de `max_nm` (max_nm doit rester < 60 nm)."""
        best, best_d = None, max_nm
        ci, cj = int(lat // 1), int(lon // 1)
        for di in (-1, 0, 1):
            for dj in (-2, -1, 0, 1, 2):
                for p in self._grid.get((ci + di, cj + dj), ()):
                    d = haversine_nm(lat, lon, p.lat, p.lon)
                    if d <= best_d:
                        best, best_d = p, d
        return (best, best_d) if best else None


def _parse_digitraffic(payload: dict) -> list[Port]:
    ports = []
    for f in payload.get("ssnLocations", {}).get("features", []):
        geom = f.get("geometry")
        if not geom:
            continue
        lon, lat = geom["coordinates"][:2]
        props = f.get("properties", {})
        ports.append(Port(props["locode"], props.get("locationName", props["locode"]),
                          props.get("country", ""), float(lat), float(lon)))
    # Ports finlandais/baltes d'abord pour prioriser les homonymes proches de la zone couverte
    ports.sort(key=lambda p: (p.country not in ("Finland", "Sweden", "Estonia", "Russia"), p.locode))
    return ports


def load_ports(cache_path: str, user_agent: str = "vigie") -> PortIndex:
    payload = None
    if os.path.exists(cache_path):
        with open(cache_path, encoding="utf-8") as fh:
            payload = json.load(fh)
    else:
        try:
            r = requests.get(PORTS_URL, headers={"Digitraffic-User": user_agent,
                                                 "Accept-Encoding": "gzip"}, timeout=30)
            r.raise_for_status()
            payload = r.json()
            os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
            with open(cache_path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
        except Exception as exc:  # réseau indisponible : liste de secours
            log.warning("Gazetteer Digitraffic indisponible (%s), liste de secours utilisée", exc)
    ports = _parse_digitraffic(payload) if payload else []
    known = {p.locode for p in ports}
    ports += [Port(*row) for row in _FALLBACK if row[0] not in known]
    log.info("Gazetteer : %d ports chargés", len(ports))
    return PortIndex(ports)
