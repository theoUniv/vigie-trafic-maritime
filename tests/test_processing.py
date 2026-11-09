from datetime import datetime, timedelta, timezone

import pytest

from maritime.ais import (eta_to_datetime, flag_of, mid_of, ship_category,
                          unpack_digitraffic_eta)
from maritime.geo import cell_bounds, cell_of, haversine_nm
from maritime.ports import Port, PortIndex
from maritime.processing import anomalies as an
from maritime.processing.congestion import CongestionTracker, VesselSnapshot
from maritime.processing.eta import estimate
from maritime.processing.state import MINUTE_MS, Point, VesselTrack
from maritime.sources.digitraffic import normalize_location, normalize_vessel

T0 = 1_800_000_000_000
HELSINKI = Port("FIHEL", "Helsinki", "Finland", 60.1567, 24.9550)
TALLINN = Port("EETLL", "Tallinn", "Estonia", 59.4450, 24.7650)
PORTS = PortIndex([HELSINKI, TALLINN, Port("FIHKO", "Hanko", "Finland", 59.8236, 22.9681)])


def track_from(points):
    t = VesselTrack(230000001)
    for i, (lat, lon, sog) in enumerate(points):
        t.add(Point(T0 + i * MINUTE_MS, lat, lon, sog, 90.0, None, 0))
    return t


# --------------------------------------------------------------------- géographie / AIS
def test_haversine_helsinki_tallinn():
    assert haversine_nm(HELSINKI.lat, HELSINKI.lon, TALLINN.lat, TALLINN.lon) == pytest.approx(43, abs=1.5)


def test_cell_roundtrip():
    i, j = cell_of(60.2, 24.9, 5.0)
    lat_min, lon_min, lat_max, lon_max = cell_bounds(i, j, 5.0)
    assert lat_min <= 60.2 < lat_max and lon_min <= 24.9 < lon_max
    # cellule ~carrée : 5 km de haut comme de large
    assert (lat_max - lat_min) * 111.32 == pytest.approx(5, rel=0.01)
    assert haversine_nm(60.2, lon_min, 60.2, lon_max) * 1.852 == pytest.approx(5, rel=0.05)


@pytest.mark.parametrize("mmsi,iso", [(230617000, "FI"), (273218530, "RU"), (636012345, "LR"),
                                      (538001234, "MH"), (227000000, "FR"), (111230123, "FI"),
                                      (992301234, "FI"), (123456789, "XX")])
def test_flag_of(mmsi, iso):
    assert flag_of(mmsi)[0] == iso


def test_mid_of_invalid():
    assert mid_of(12345) is None


@pytest.mark.parametrize("t,cat", [(70, "cargo"), (89, "tanker"), (60, "passenger"), (30, "fishing"),
                                   (52, "service"), (37, "pleasure"), (0, "other"), (None, "other")])
def test_ship_category(t, cat):
    assert ship_category(t) == cat


def test_unpack_eta_and_year_choice():
    raw = (11 << 16) | (10 << 11) | (4 << 6) | 30  # 10 nov. 04:30
    eta = unpack_digitraffic_eta(raw)
    assert eta == {"month": 11, "day": 10, "hour": 4, "minute": 30}
    now = datetime(2026, 11, 9, 12, tzinfo=timezone.utc)
    assert eta_to_datetime(eta, now) == datetime(2026, 11, 10, 4, 30, tzinfo=timezone.utc)
    # ETA en janvier vue depuis décembre : année suivante
    jan = {"month": 1, "day": 3, "hour": 8, "minute": 0}
    assert eta_to_datetime(jan, datetime(2026, 12, 28, tzinfo=timezone.utc)).year == 2027
    assert eta_to_datetime({"month": 0, "day": 0, "hour": 24, "minute": 60}, now) is None
    assert unpack_digitraffic_eta(0) is None


def test_normalize_digitraffic():
    feat = {"mmsi": 230987380, "geometry": {"coordinates": [21.39, 60.43]},
            "properties": {"sog": 102.3, "cog": 360, "heading": 511, "navStat": 0,
                           "timestampExternal": T0}}
    msg = normalize_location(feat)
    assert msg["sog"] is None and msg["cog"] is None and msg["heading"] is None
    assert (msg["lat"], msg["lon"]) == (60.43, 21.39)
    v = normalize_vessel({"mmsi": 1, "name": " X ", "draught": 30, "referencePointA": 120,
                          "referencePointB": 21, "referencePointC": 11, "referencePointD": 6,
                          "eta": 0, "destination": ""})
    assert v["name"] == "X" and v["draught"] == 3.0 and v["length"] == 141 and v["width"] == 17
    assert v["destination"] is None and v["eta"] is None


# ------------------------------------------------------------------------------ ports
@pytest.mark.parametrize("dest,locode", [("FIHEL", "FIHEL"), ("FI HEL", "FIHEL"), ("HELSINKI", "FIHEL"),
                                         ("FI/HANKO", "FIHKO"), ("EE TLL>FI HEL", "FIHEL"),
                                         ("PORT OF TALLINN", "EETLL"), ("FIHEL VUOSAARI", "FIHEL")])
def test_resolve_destination(dest, locode):
    assert PORTS.resolve(dest).locode == locode


@pytest.mark.parametrize("dest", [None, "", "FOR ORDERS", "SESTO<>FIMHQ<>FIHEL", "-"])
def test_resolve_destination_unknown(dest):
    assert PORTS.resolve(dest) is None


def test_nearest_port():
    port, dist = PORTS.nearest(60.15, 24.96, 5)
    assert port.locode == "FIHEL" and dist < 1
    assert PORTS.nearest(59.0, 21.0, 5) is None


# -------------------------------------------------------------------------- anomalies
def test_track_rejects_out_of_order():
    t = track_from([(60, 24, 10), (60.01, 24, 10)])
    assert not t.add(Point(T0, 60, 24, 10, 0, None, 0))
    assert len(t.points) == 2


def test_position_jump_detected():
    t = track_from([(60.0, 24.0, 12), (61.0, 24.0, 12)])  # 60 nm en 1 min
    alert = an.check_position_jump(t)
    assert alert and alert.kind == "POSITION_JUMP"


def test_no_jump_for_normal_navigation():
    t = track_from([(60.0, 24.0, 12), (60.0033, 24.0, 12)])  # 0.2 nm en 1 min = 12 nd
    assert an.check_position_jump(t) is None


def test_slowdown_detected_at_sea_only():
    pts = [(59.5 + i * 0.002, 22.0, 13.0) for i in range(20)] + [(59.54, 22.0, 1.0)]
    t = track_from(pts)
    assert an.check_slowdown(t, "cargo", near_port=False).kind == "SLOWDOWN"
    assert an.check_slowdown(t, "cargo", near_port=True) is None
    assert an.check_slowdown(t, "fishing", near_port=False) is None


def test_unexpected_stop():
    t = track_from([(59.5, 22.0, 0.1)] * 25)
    alert = an.check_unexpected_stop(t, "tanker", near_port=False)
    assert alert and alert.details["stopped_min"] >= 20
    short = track_from([(59.5, 22.0, 0.1)] * 5)
    assert an.check_unexpected_stop(short, "tanker", near_port=False) is None


def test_ais_gap():
    t = track_from([(59.5, 22.0, 12)] * 3)
    last = t.last.ts
    assert an.check_ais_gap(t, last + 30 * MINUTE_MS, 20).kind == "AIS_GAP"
    assert an.check_ais_gap(t, last + 5 * MINUTE_MS, 20) is None
    assert an.check_ais_gap(track_from([(59.5, 22.0, 12)]), last + 30 * MINUTE_MS, 20) is None


# ------------------------------------------------------------------------- congestion
def _fleet(n, sog, nav=1, cat="cargo", lat=59.86, lon=28.38):
    return [VesselSnapshot(i, lat + i * 0.001, lon + i * 0.001, sog, nav, cat) for i in range(n)]


def test_congestion_anchorage_queue():
    cells = CongestionTracker(5.0, 3).compute(_fleet(6, 0.1))
    assert cells[0].level == "congestion" and cells[0].n_waiting == 6


def test_tugs_in_harbour_are_not_congestion():
    cells = CongestionTracker(5.0, 3).compute(_fleet(8, 0.0, nav=0, cat="service"))
    assert cells[0].level != "congestion"


def test_moored_vessels_ignored():
    assert CongestionTracker(5.0, 3).compute(_fleet(6, 0.0, nav=5)) == []


def test_slowdown_against_baseline():
    tracker = CongestionTracker(5.0, 3)
    fleet_fast = _fleet(4, 14.0, nav=0)
    tracker.compute(fleet_fast)
    cells = tracker.compute(_fleet(4, 4.0, nav=0))
    assert cells[0].level == "ralentissement"
    assert cells[0].baseline_sog == pytest.approx(14.0)


# ------------------------------------------------------------------------------- ETA
NOW = datetime(2026, 11, 9, 12, tzinfo=timezone.utc)


def test_eta_on_time():
    # ~43 nm * 1.25 = 54 nm à 12 nd = 4.5 h ; ETA déclarée dans 4 h 30
    est = estimate(NOW, TALLINN.lat, TALLINN.lon, 12.0, "passenger", HELSINKI, NOW + timedelta(hours=4.5))
    assert est["status"] == "a_l_heure" and abs(est["delay_min"]) < 30


def test_eta_delay():
    est = estimate(NOW, TALLINN.lat, TALLINN.lon, 6.0, "cargo", HELSINKI, NOW + timedelta(hours=2))
    assert est["status"] == "retard_important" and est["delay_min"] > 240


def test_eta_arrived_and_stopped():
    assert estimate(NOW, HELSINKI.lat, HELSINKI.lon, 0, "cargo", HELSINKI, None)["status"] == "arrive"
    est = estimate(NOW, TALLINN.lat, TALLINN.lon, 0.2, "cargo", HELSINKI, None)
    assert est["status"] == "sans_eta" and est["speed_used"] == 12.0
