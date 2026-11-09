"""Référentiels AIS : types de navires, statuts de navigation, pavillons (MID), ETA."""
from __future__ import annotations

from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Types de navires (champ "ship type" AIS, ITU-R M.1371)
# ---------------------------------------------------------------------------
CATEGORIES = {
    "cargo": "Cargo",
    "tanker": "Pétrolier / chimiquier",
    "passenger": "Passagers",
    "fishing": "Pêche",
    "service": "Remorqueur / service",
    "highspeed": "Grande vitesse",
    "pleasure": "Plaisance / voile",
    "authority": "Militaire / autorité",
    "other": "Autre / inconnu",
}
COMMERCIAL_CATEGORIES = {"cargo", "tanker", "passenger"}

# Vitesse de croisière typique (nœuds) utilisée quand un navire est à l'arrêt
TYPICAL_SPEED_KN = {
    "cargo": 12.0, "tanker": 11.0, "passenger": 16.0, "fishing": 8.0, "service": 8.0,
    "highspeed": 25.0, "pleasure": 6.0, "authority": 12.0, "other": 10.0,
}


def ship_category(ship_type: int | None) -> str:
    t = ship_type or 0
    if 70 <= t <= 79:
        return "cargo"
    if 80 <= t <= 89:
        return "tanker"
    if 60 <= t <= 69:
        return "passenger"
    if t == 30:
        return "fishing"
    if 40 <= t <= 49:
        return "highspeed"
    if t in (36, 37):
        return "pleasure"
    if t in (35, 55):
        return "authority"
    if t in (31, 32, 33, 34, 50, 51, 52, 53, 54, 58, 59):
        return "service"
    return "other"


SHIP_TYPE_LABELS = {
    30: "Pêche", 31: "Remorquage", 32: "Remorquage (grand)", 33: "Dragage", 34: "Plongée",
    35: "Militaire", 36: "Voilier", 37: "Plaisance", 50: "Pilote", 51: "Recherche & sauvetage",
    52: "Remorqueur", 53: "Navette portuaire", 54: "Antipollution", 55: "Forces de l'ordre",
    58: "Transport médical",
}


def ship_type_label(ship_type: int | None) -> str:
    t = ship_type or 0
    if t in SHIP_TYPE_LABELS:
        return SHIP_TYPE_LABELS[t]
    return CATEGORIES[ship_category(t)]


# ---------------------------------------------------------------------------
# Statut de navigation
# ---------------------------------------------------------------------------
NAV_STATUS = {
    0: "En route (moteur)", 1: "Au mouillage", 2: "Non maître de sa manœuvre",
    3: "Manœuvrabilité restreinte", 4: "Handicapé par son tirant d'eau", 5: "Amarré",
    6: "Échoué", 7: "En pêche", 8: "En route (voile)", 14: "AIS-SART actif", 15: "Non défini",
}
UNDERWAY_STATUSES = {0, 8}
MOORED, AT_ANCHOR = 5, 1


def nav_status_label(code: int | None) -> str:
    return NAV_STATUS.get(code if code is not None else 15, "Non défini")


# ---------------------------------------------------------------------------
# Pavillon : MID (Maritime Identification Digits) -> pays
# ---------------------------------------------------------------------------
_MID_TABLE = """
201 AL Albanie|202 AD Andorre|203 AT Autriche|204 PT Portugal (Açores)|205 BE Belgique|206 BY Biélorussie
207 BG Bulgarie|208 VA Vatican|209-210 CY Chypre|211 DE Allemagne|212 CY Chypre|213 GE Géorgie
214 MD Moldavie|215 MT Malte|216 AM Arménie|218 DE Allemagne|219-220 DK Danemark|224-225 ES Espagne
226-228 FR France|229 MT Malte|230 FI Finlande|231 FO Îles Féroé|232-235 GB Royaume-Uni|236 GI Gibraltar
237 GR Grèce|238 HR Croatie|239-241 GR Grèce|242 MA Maroc|243 HU Hongrie|244-246 NL Pays-Bas
247 IT Italie|248-249 MT Malte|250 IE Irlande|251 IS Islande|252 LI Liechtenstein|253 LU Luxembourg
254 MC Monaco|255 PT Portugal (Madère)|256 MT Malte|257-259 NO Norvège|261 PL Pologne|262 ME Monténégro
263 PT Portugal|264 RO Roumanie|265-266 SE Suède|267 SK Slovaquie|268 SM Saint-Marin|269 CH Suisse
270 CZ Tchéquie|271 TR Turquie|272 UA Ukraine|273 RU Russie|274 MK Macédoine du Nord|275 LV Lettonie
276 EE Estonie|277 LT Lituanie|278 SI Slovénie|279 RS Serbie
301 AI Anguilla|303 US États-Unis|304-305 AG Antigua-et-Barbuda|306 CW Curaçao|307 AW Aruba
308-309 BS Bahamas|310 BM Bermudes|311 BS Bahamas|312 BZ Belize|314 BB Barbade|316 CA Canada
319 KY Îles Caïmans|321 CR Costa Rica|323 CU Cuba|325 DM Dominique|327 DO République dominicaine
329 GP Guadeloupe|330 GD Grenade|331 GL Groenland|332 GT Guatemala|334 HN Honduras|336 HT Haïti
338 US États-Unis|339 JM Jamaïque|341 KN Saint-Christophe-et-Niévès|343 LC Sainte-Lucie|345 MX Mexique
347 MQ Martinique|348 MS Montserrat|350 NI Nicaragua|351-357 PA Panama|358 PR Porto Rico
359 SV Salvador|361 PM Saint-Pierre-et-Miquelon|362 TT Trinité-et-Tobago|364 TC Îles Turques-et-Caïques
366-369 US États-Unis|370-374 PA Panama|375-377 VC Saint-Vincent-et-les-Grenadines
378 VG Îles Vierges britanniques|379 VI Îles Vierges américaines
401 AF Afghanistan|403 SA Arabie saoudite|405 BD Bangladesh|408 BH Bahreïn|410 BT Bhoutan
412-414 CN Chine|416 TW Taïwan|417 LK Sri Lanka|419 IN Inde|422 IR Iran|423 AZ Azerbaïdjan|425 IQ Irak
428 IL Israël|431-432 JP Japon|434 TM Turkménistan|436 KZ Kazakhstan|437 UZ Ouzbékistan|438 JO Jordanie
440-441 KR Corée du Sud|443 PS Palestine|445 KP Corée du Nord|447 KW Koweït|450 LB Liban
451 KG Kirghizistan|453 MO Macao|455 MV Maldives|457 MN Mongolie|459 NP Népal|461 OM Oman|463 PK Pakistan
466 QA Qatar|468 SY Syrie|470-471 AE Émirats arabes unis|472 TJ Tadjikistan|473 YE Yémen|475 YE Yémen
477 HK Hong Kong|478 BA Bosnie-Herzégovine
501 TF Terre Adélie|503 AU Australie|506 MM Birmanie|508 BN Brunei|510 FM Micronésie|511 PW Palaos
512 NZ Nouvelle-Zélande|514-515 KH Cambodge|516 CX Île Christmas|518 CK Îles Cook|520 FJ Fidji
523 CC Îles Cocos|525 ID Indonésie|529 KI Kiribati|531 LA Laos|533 MY Malaisie|536 MP Îles Mariannes du Nord
538 MH Îles Marshall|540 NC Nouvelle-Calédonie|542 NU Niue|544 NR Nauru|546 PF Polynésie française
548 PH Philippines|550 TL Timor oriental|553 PG Papouasie-Nouvelle-Guinée|555 PN Pitcairn
557 SB Îles Salomon|559 AS Samoa américaines|561 WS Samoa|563-566 SG Singapour|567 TH Thaïlande
570 TO Tonga|572 TV Tuvalu|574 VN Viêt Nam|576-577 VU Vanuatu|578 WF Wallis-et-Futuna
601 ZA Afrique du Sud|603 AO Angola|605 DZ Algérie|607 TF Saint-Paul-et-Amsterdam|608 SH Ascension
609 BI Burundi|610 BJ Bénin|611 BW Botswana|612 CF Centrafrique|613 CM Cameroun|615 CG Congo
616 KM Comores|617 CV Cap-Vert|618 TF Crozet|619 CI Côte d'Ivoire|620 KM Comores|621 DJ Djibouti
622 EG Égypte|624 ET Éthiopie|625 ER Érythrée|626 GA Gabon|627 GH Ghana|629 GM Gambie
630 GW Guinée-Bissau|631 GQ Guinée équatoriale|632 GN Guinée|633 BF Burkina Faso|634 KE Kenya
635 TF Kerguelen|636-637 LR Liberia|638 SS Soudan du Sud|642 LY Libye|644 LS Lesotho|645 MU Maurice
647 MG Madagascar|649 ML Mali|650 MZ Mozambique|654 MR Mauritanie|655 MW Malawi|656 NE Niger
657 NG Nigeria|659 NA Namibie|660 RE La Réunion|661 RW Rwanda|662 SD Soudan|663 SN Sénégal
664 SC Seychelles|665 SH Sainte-Hélène|666 SO Somalie|667 SL Sierra Leone|668 ST Sao Tomé-et-Principe
669 SZ Eswatini|670 TD Tchad|671 TG Togo|672 TN Tunisie|674 TZ Tanzanie|675 UG Ouganda
676 CD RD Congo|677 TZ Tanzanie|678 ZM Zambie|679 ZW Zimbabwe
701 AR Argentine|710 BR Brésil|720 BO Bolivie|725 CL Chili|730 CO Colombie|735 EC Équateur
740 FK Îles Malouines|745 GF Guyane|750 GY Guyana|755 PY Paraguay|760 PE Pérou|765 SR Suriname
770 UY Uruguay|775 VE Venezuela
"""


def _parse_mid_table(raw: str) -> dict[int, tuple[str, str]]:
    table: dict[int, tuple[str, str]] = {}
    for entry in raw.replace("\n", "|").split("|"):
        entry = entry.strip()
        if not entry:
            continue
        mids, iso, name = entry.split(" ", 2)
        lo, _, hi = mids.partition("-")
        for mid in range(int(lo), int(hi or lo) + 1):
            table[mid] = (iso, name)
    return table


MID_COUNTRIES = _parse_mid_table(_MID_TABLE)
UNKNOWN_COUNTRY = ("XX", "Inconnu")


def mid_of(mmsi: int) -> int | None:
    """Extrait le MID d'un MMSI selon le préfixe (navire, aide à la navigation, SAR...)."""
    s = str(mmsi)
    if len(s) != 9:
        return None
    if s.startswith("111"):          # aéronef SAR : 111MIDxxx
        return int(s[3:6])
    if s.startswith(("98", "99")):   # engin associé / aide à la navigation : 98MIDxxxx
        return int(s[2:5])
    if s.startswith("0"):            # station côtière / groupe : 00MID / 0MID
        return int(s[2:5]) if s.startswith("00") else int(s[1:4])
    if s[0] in "234567":
        return int(s[:3])
    return None


def flag_of(mmsi: int) -> tuple[str, str]:
    """(code ISO2, nom du pays) du pavillon déduit du MMSI."""
    mid = mid_of(mmsi)
    return MID_COUNTRIES.get(mid, UNKNOWN_COUNTRY) if mid else UNKNOWN_COUNTRY


# ---------------------------------------------------------------------------
# ETA AIS (mois / jour / heure / minute, sans année)
# ---------------------------------------------------------------------------
def unpack_digitraffic_eta(raw: int | None) -> dict | None:
    """Décode l'ETA « bit-packed » de Digitraffic (MMDDHHMM sur 20 bits)."""
    if not raw:
        return None
    month = (raw >> 16) & 0xF
    day = (raw >> 11) & 0x1F
    hour = (raw >> 6) & 0x1F
    minute = raw & 0x3F
    return {"month": month, "day": day, "hour": hour, "minute": minute}


def eta_to_datetime(eta: dict | None, now: datetime) -> datetime | None:
    """Convertit une ETA AIS (sans année) en datetime UTC, en choisissant l'année
    qui place la date au plus près de `now`. Renvoie None si l'ETA n'est pas renseignée."""
    if not eta:
        return None
    month, day = eta.get("month", 0), eta.get("day", 0)
    hour, minute = eta.get("hour", 24), eta.get("minute", 60)
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return None
    hour = 0 if hour >= 24 else hour
    minute = 0 if minute >= 60 else minute
    best = None
    for year in (now.year - 1, now.year, now.year + 1):
        try:
            candidate = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
        except ValueError:
            continue
        if best is None or abs(candidate - now) < abs(best - now):
            best = candidate
    return best
