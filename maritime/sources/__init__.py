"""Sources de données AIS.

Chaque source expose `snapshots()`, un générateur qui produit toutes les
`poll_interval_s` secondes un lot normalisé :
    {"positions": [PositionMsg, ...], "static": [StaticMsg, ...]}

PositionMsg : {mmsi, ts (epoch ms), lat, lon, sog, cog, heading, nav_status, rot, source}
StaticMsg   : {mmsi, ts, name, callsign, imo, ship_type, destination, eta{month,day,hour,minute},
               draught, length, width, source}
"""
