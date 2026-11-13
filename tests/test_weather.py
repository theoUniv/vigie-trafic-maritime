"""Tests de la météo marine Open-Meteo : lecture des réponses, état de la mer, enrichissement des zones."""
import pytest

from maritime.llm.briefing import build_facts
from maritime.processing.weather import WeatherIndex, beaufort, sea_state, weather_note
from maritime.processor import Processor
from maritime.sources.openmeteo import _as_list, _parse_ts, build_grid, merge_observations


def obs(lat=60.0, lon=25.0, wind=10.0, wave=0.8, gust=None, vis=30000.0, code=3):
    return {"lat": lat, "lon": lon, "ts": 1_800_000_000_000, "wind_kn": wind, "wind_dir": 200,
            "gust_kn": gust, "visibility_m": vis, "weather_code": code, "wave_m": wave}


# ------------------------------------------------------------------ Open-Meteo
def test_build_grid_includes_both_edges():
    g = build_grid("59,20,60,22", "0.5,1")
    assert len(g) == 3 * 3
    assert g[0] == (59.0, 20.0) and g[-1] == (60.0, 22.0)


def test_merge_drops_land_points_and_keeps_order():
    points = [(60.0, 25.0), (61.0, 26.0), (59.5, 24.0)]
    weather = [{"current": {"time": "2026-11-12T14:00", "wind_speed_10m": 20.4, "wind_direction_10m": 173,
                            "wind_gusts_10m": 26.4, "visibility": 56140.0, "weather_code": 3}},
               {"current": {"time": "2026-11-12T14:00", "wind_speed_10m": 9.0}},
               {"current": {"time": "2026-11-12T14:00", "wind_speed_10m": 12.0}}]
    marine = [{"current": {"time": "2026-11-12T14:00", "wave_height": 1.26, "wave_period": 4.4,
                           "sea_surface_temperature": 10.7}},
              {"current": {"time": "2026-11-12T14:00", "wave_height": None}},      # à terre
              {"current": {"time": "2026-11-12T14:00", "wave_height": 0.5}}]
    out = merge_observations(points, weather, marine)
    assert [(o["lat"], o["lon"]) for o in out] == [(60.0, 25.0), (59.5, 24.0)]
    first = out[0]
    assert first["wind_kn"] == 20.4 and first["gust_kn"] == 26.4 and first["wave_m"] == 1.26
    assert first["sea_temp_c"] == 10.7 and first["source"] == "open-meteo"
    assert first["ts"] == _parse_ts("2026-11-12T14:00")


def test_single_point_response_is_an_object():
    assert _as_list({"current": {}}) == [{"current": {}}]
    assert _as_list([{"a": 1}, {"b": 2}]) == [{"a": 1}, {"b": 2}]


def test_parse_ts_is_gmt():
    assert _parse_ts("1970-01-01T01:00") == 3_600_000
    assert _parse_ts(None) is None


# ------------------------------------------------------------- état de la mer
@pytest.mark.parametrize("wind,force", [(0.5, 0), (5, 2), (21.9, 5), (22, 6), (34, 8), (70, 12), (None, None)])
def test_beaufort(wind, force):
    assert beaufort(wind) == force


def test_sea_state_levels_and_causes():
    calm = sea_state(obs(wind=8, wave=0.4))
    assert calm["level"] == 0 and calm["label"] == "calme" and calm["causes"] == []
    assert sea_state(obs(wind=18, wave=0.9))["label"] == "agitée"
    hard = sea_state(obs(wind=25, wave=2.8))
    assert hard["level"] == 2 and hard["label"] == "difficile"
    assert hard["causes"] == ["vent fort (25,0 nd, force 6)", "mer forte (2,8 m)"]
    assert hard["summary"] == "vent 25,0 nd, vagues 2,8 m"
    assert sea_state(obs(wind=36))["label"] == "dangereuse"
    assert sea_state(obs(wind=10, wave=4.5))["level"] == 3
    fog = sea_state(obs(vis=400, code=45))
    assert fog["level"] == 2 and "visibilité réduite (0,4 km)" in fog["causes"] and fog["sky"] == "brouillard"
    assert sea_state(None) is None


def test_weather_note_only_when_conditions_are_hard():
    assert weather_note(obs(wind=10, wave=0.5)) == ""
    assert weather_note(obs(wind=28, wave=1.0)) == " — météo : vent fort (28,0 nd, force 7)"


def test_weather_index_nearest_with_distance_limit():
    idx = WeatherIndex(max_km=60)
    idx.update(obs(lat=60.0, lon=25.0, wind=5))
    idx.update(obs(lat=60.0, lon=27.0, wind=30))
    near = idx.nearest(60.1, 26.8)
    assert near["wind_kn"] == 30 and near["distance_km"] < 20
    assert idx.nearest(63.0, 20.0) is None                 # trop loin de tout point
    idx.update(obs(lat=60.0, lon=27.0, wind=12))           # nouvelle observation au même point
    assert len(idx) == 2 and idx.nearest(60.0, 27.0)["wind_kn"] == 12


# ------------------------------------------------- processeur et bulletin
def test_processor_attaches_weather_to_zones():
    proc = Processor(writer=None, producer=None)
    proc.handle_weather(obs(lat=60.0, lon=28.5, wind=27, wave=2.6))
    proc.handle_weather({"lat": None})                      # message incomplet ignoré
    assert len(proc.weather) == 1 and len(proc.pending_weather) == 1
    w = proc.zone_weather(59.95, 28.4)
    assert w["label"] == "difficile" and w["wind_kn"] == 27 and w["distance_km"] < 15
    assert proc.zone_weather(54.0, 12.0) is None
    proc.flush()
    assert proc.pending_weather == []


def test_briefing_facts_carry_zone_weather():
    w = sea_state(obs(wind=27, wave=2.6))
    cell = {"level": "ralentissement", "score": 3, "place": "Ust'-Luga", "lat_min": 59.6, "lat_max": 59.65,
            "lon_min": 28.4, "lon_max": 28.5, "n_vessels": 5, "n_waiting": 2, "mean_sog": 3.1,
            "baseline_sog": 9.0, "weather": w}
    zone = build_facts([], [cell], {}, 60, NOW := 1_800_000_000_000)["zones"][0]
    assert zone["meteo"].startswith("mer difficile : vent 27,0 nd, vagues 2,6 m (vent fort")
    calm_cell = {**cell, "weather": None}
    assert "meteo" not in build_facts([], [calm_cell], {}, 60, NOW)["zones"][0]
