# Vigie — suivi du trafic maritime en temps réel

> Projet IPSSI, module Open Data. **Sujet 3 : « Suivez le flux maritime en temps réel »**

Vigie collecte chaque minute les positions AIS des navires depuis une API open data et les publie
dans **Apache Kafka**. Un processeur de flux les exploite pour détecter les **zones de
congestion et de ralentissement**, les **retards probables** et les **comportements anormaux**.
Le tout est restitué sur une **carte web interactive** avec filtres et suivi d'un navire.

![Interface Vigie](docs/figures/00_interface.png)

---

## Objectifs du sujet et réalisation

| Objectif de l'énoncé | Réalisation |
|---|---|
| Utiliser une API qui fournit en live les données des bateaux | API **Digitraffic** (open data, sans clé, licence CC BY 4.0). La source **aisstream.io** (couverture mondiale, clé gratuite) est aussi prise en charge |
| Collecter les positions toutes les minutes | `maritime/producer.py` : un appel par minute, positions dédoublonnées (incrémental via `from=`) et fiches navires toutes les 10 min |
| Publier les données dans Apache Kafka | Topics `ais.positions` (3 partitions, clé = MMSI), `ais.static`, `ais.alerts` et `ais.congestion` |
| Détecter les zones de ralentissement ou de congestion | Grille d'environ 5 km : file de cargos au mouillage (**congestion**) ou vitesse inférieure à 50 % de la normale apprise (**ralentissement**) |
| Détecter les retards | Comparaison entre l'ETA déclarée par l'équipage et l'ETA recalculée (distance restante / vitesse moyenne) |
| Détecter les anomalies | Saut de position impossible, ralentissement brutal en mer, arrêt en mer, perte du signal AIS |
| Suivre un navire en particulier | Fiche navire, trajectoire sur 2 h, 6 h ou 24 h, profil de vitesse et mode **« Suivre »** (la carte reste centrée) |
| Visualiser le trafic mondial ou régional sur une carte | Carte Leaflet en canvas (plusieurs milliers de navires orientés selon leur cap), couches zones, alertes et balisage OpenSeaMap |
| Interface dynamique avec filtres (nom, type, pays) | Recherche (nom, MMSI, IMO, indicatif, destination), filtres type, pavillon, état et commerce, actualisation toutes les 15 s |
| Analyse et modèle de prédiction (rappels) | Notebook `notebooks/analyse_trafic.ipynb` : data viz matplotlib, **prévision de position à 10 min** et **Isolation Forest** |

---

## Architecture

```mermaid
flowchart LR
    subgraph Sources["Sources open data"]
        DT["Digitraffic AIS<br/>REST · Baltique"]
        AS["aisstream.io<br/>WebSocket · mondial"]
        PT["Gazetteer des ports<br/>12 000 UN/LOCODE"]
    end

    subgraph Ingestion
        P["producer<br/>collecte / 60 s"]
    end

    subgraph Kafka["Apache Kafka (KRaft)"]
        T1[("ais.positions")]
        T2[("ais.static")]
        T3[("ais.alerts")]
        T4[("ais.congestion")]
    end

    subgraph Traitement
        PR["processor<br/>état des navires<br/>congestion · ETA · anomalies"]
    end

    DB[("PostgreSQL<br/>état temps réel<br/>+ historique")]
    API["API FastAPI"]
    WEB["Carte web<br/>Leaflet"]
    NB["Notebook<br/>analyse + modèles"]
    UI["Kafka UI"]

    DT --> P
    AS -.-> P
    P --> T1 & T2
    T1 & T2 --> PR
    PT --> PR
    PR --> T3 & T4
    PR --> DB
    DB --> API --> WEB
    DB --> NB
    Kafka --- UI
```

| Service | Rôle | Port |
|---|---|---|
| `kafka` | Broker Apache Kafka 3.8 en mode KRaft (sans ZooKeeper) | 9092 |
| `kafka-ui` | Exploration des topics et des messages | [8080](http://localhost:8080) |
| `producer` | Collecte AIS → Kafka | – |
| `processor` | Kafka → détection → PostgreSQL et topics d'alertes | – |
| `postgres` | Stockage de l'état courant, de l'historique et des alertes | 5433 |
| `api` | API REST et interface web | [8000](http://localhost:8000) |

**Pourquoi ce découpage ?** Kafka découple la collecte du traitement. Si le processeur
redémarre, il reprend à son dernier offset sans perdre de message, et il recharge son état récent
(trajectoires, vitesses de référence, alertes déjà émises) depuis PostgreSQL. La clé de partition
MMSI garantit que les positions d'un même navire sont traitées dans l'ordre. On peut ajouter
d'autres consommateurs (archivage, Spark, alerting) sans toucher au producteur.

---

## Démarrage rapide

Prérequis : Docker Desktop (ou Docker Engine et Compose v2).

```bash
git clone https://github.com/theoUniv/vigie-trafic-maritime.git
cd vigie-trafic-maritime
docker compose up -d --build
```

- Carte : **http://localhost:8000** (premiers navires visibles en moins d'une minute)
- Documentation de l'API : http://localhost:8000/docs
- Kafka UI : http://localhost:8080

Les détections gagnent en pertinence après une trentaine de minutes : il faut le temps
d'accumuler des trajectoires et d'apprendre les vitesses de référence des zones.

```bash
docker compose logs -f producer processor   # suivre le pipeline
docker compose down                         # arrêter (ajouter -v pour effacer les données)
```

### Passer au trafic mondial (aisstream.io)

```bash
cp .env.example .env
# dans .env : AIS_SOURCE=aisstream, AISSTREAM_API_KEY=<votre clé>, AISSTREAM_BBOXES=...
docker compose up -d producer
```

Le flux WebSocket continu est regroupé en instantanés d'une minute (dernière position de chaque
navire). Le reste du pipeline est identique.

> La source par défaut (Digitraffic) a été testée de bout en bout. La source aisstream suit la
> documentation publique d'aisstream.io mais n'a pas pu être testée faute de clé.

---

## Données

| Donnée | Source | Licence |
|---|---|---|
| Positions et fiches navires AIS | [Digitraffic — Fintraffic](https://www.digitraffic.fi/en/marine-traffic/) (`/api/ais/v1/locations`, `/vessels`) | CC BY 4.0 |
| Positions AIS mondiales (optionnel) | [aisstream.io](https://aisstream.io) | Conditions aisstream |
| Ports (UN/LOCODE et coordonnées) | Digitraffic Port Call API | CC BY 4.0 |
| Balisage maritime | [OpenSeaMap](https://www.openseamap.org) | CC BY-SA |
| Fond de carte | Esri World Gray Canvas | Attribution |
| Pavillon | Déduit du MMSI (MID, table ITU) | – |

### Messages Kafka

```jsonc
// ais.positions — clé : MMSI
{"mmsi": 230617000, "ts": 1794239108111, "lat": 60.214, "lon": 25.202, "sog": 11.3,
 "cog": 87.2, "heading": 88, "nav_status": 0, "rot": 0, "source": "digitraffic",
 "ingested_at": 1794239110000}

// ais.static — clé : MMSI
{"mmsi": 273218530, "name": "POLA ANASTASIA", "ship_type": 70, "callsign": "UFON",
 "imo": 9897690, "destination": "RU LED", "eta": {"month": 11, "day": 10, "hour": 4, "minute": 30},
 "draught": 3.0, "length": 141, "width": 17, "source": "digitraffic"}

// ais.alerts
{"kind": "SLOWDOWN", "severity": "warning", "mmsi": 246554000, "lat": 59.62, "lon": 22.9,
 "message": "Vitesse passée de 12.4 à 0.8 nœuds hors zone portuaire", "label": "Ralentissement brutal"}
```

---

## Logique de détection

Toute la logique est dans `maritime/processing/`. Ce sont des fonctions pures, sans Kafka ni
base de données, couvertes par des tests unitaires.

| Détection | Règle | Paramètres |
|---|---|---|
| **Congestion** | Dans une cellule d'environ 5 km, au moins 3 cargos ou pétroliers à l'arrêt ou au mouillage, représentant au moins 60 % des navires de charge présents. Les remorqueurs et ferries à quai sont ignorés | `CELL_KM`, `CONGESTION_MIN_VESSELS` |
| **Ralentissement de zone** | Vitesse moyenne de la cellule inférieure à 50 % de sa vitesse de référence (moyenne mobile exponentielle apprise en continu, rechargée sur 24 h au redémarrage) | `slowdown_ratio=0.5` |
| **Retard probable / important** | ETA recalculée (orthodromie × 1,25 / vitesse moyenne sur 30 min) postérieure de plus de 60 / 240 min à l'ETA déclarée. Alerte uniquement pour les navires de commerce à moins de 300 nm | `DETOUR_FACTOR`, seuils dans `eta.py` |
| **Saut de position** | Vitesse implicite entre deux positions supérieure à 60 nœuds et à 3 × la vitesse déclarée + 30 (erreur GPS ou usurpation AIS) | `MAX_PLAUSIBLE_KN` |
| **Ralentissement brutal** | Vitesse de croisière ≥ 8 nœuds sur les 30 dernières minutes, puis vitesse < 30 % de celle-ci, hors rayon de 5 nm autour d'un port | `SLOWDOWN_*` |
| **Arrêt en mer** | Immobile depuis au moins 20 min hors port alors que le statut AIS indique « en route » | `STOP_MIN_DURATION` |
| **Perte du signal AIS** | Navire suivi en mouvement qui n'émet plus depuis au moins 20 min | `AIS_GAP_MIN` |

Les conditions persistantes (arrêt, perte de signal, retard, congestion) ne déclenchent une alerte
qu'à leur **apparition** et se réarment à leur disparition. Un délai anti-rebond de 30 min évite
le spam d'alertes.

La destination AIS est un champ libre saisi par l'équipage (`FI HEL`, `HELSINKI`, `FI/HANKO`,
`EE TLL>FI HEL`…). Elle est résolue vers un port géolocalisé par code LOCODE, par nom normalisé
ou par alias. Les itinéraires multi-escales (`SESTO<>FIMHQ<>FIHEL`) sont volontairement ignorés.

---

## API REST

| Endpoint | Description |
|---|---|
| `GET /api/vessels?q=&category=&country=&status=&commercial=&bbox=` | Dernière position des navires actifs, avec filtres |
| `GET /api/search?q=` | Recherche par nom, MMSI, IMO, indicatif ou destination |
| `GET /api/vessels/{mmsi}` | Fiche complète : identité, position, estimation d'ETA, anomalies |
| `GET /api/vessels/{mmsi}/track?hours=6` | Trajectoire |
| `GET /api/congestion?min_level=dense` | Zones courantes : congestion, ralentissement, trafic dense |
| `GET /api/anomalies?hours=6&kind=` | Flux d'alertes |
| `GET /api/stats` | Indicateurs, répartition par type, débit d'ingestion |
| `GET /api/meta` | Valeurs disponibles pour les filtres |

Documentation interactive : http://localhost:8000/docs

---

## Analyse et modèles

Le notebook [`notebooks/analyse_trafic.ipynb`](notebooks/analyse_trafic.ipynb) lit la base
alimentée par le pipeline et exporte ses figures dans `docs/figures/`.

```bash
uv venv && uv pip install -e ".[analysis,dev]"     # ou : pip install -e ".[analysis,dev]"
.venv/bin/jupyter lab notebooks/analyse_trafic.ipynb
```

| | |
|---|---|
| ![Densité et congestion](docs/figures/03_densite_congestion.png) | ![Prévision de position](docs/figures/06_prevision_position.png) |

- **Modèle 1, prévision de position à 10 min** (*dead reckoning*). C'est la base de l'ETA
  recalculée et du suivi. Elle est évaluée sur les trajectoires réelles et comparée à une
  référence « navire immobile ».
- **Modèle 2, Isolation Forest.** Détection non supervisée de profils de navigation atypiques
  (variabilité de vitesse, changements de cap, vitesses implicites, coupures), comparée aux
  alertes produites par les règles.

---

## Tests

```bash
.venv/bin/python -m pytest
```

Les 47 tests couvrent la géographie, le décodage AIS (ETA, pavillons, valeurs sentinelles), la
résolution des destinations, les détecteurs d'anomalies, la congestion et l'estimation d'ETA.

---

## Structure du dépôt

```
maritime/
  config.py            configuration (variables d'environnement)
  ais.py               référentiels AIS : types, statuts, pavillons (MID), ETA
  geo.py               distances, grille spatiale
  ports.py             gazetteer UN/LOCODE et résolution des destinations
  kafka_io.py          topics, producteur et consommateur JSON
  sources/             digitraffic.py (REST), aisstream.py (WebSocket)
  producer.py          collecte → Kafka
  processing/          state, anomalies, congestion, eta (logique pure)
  processor.py         Kafka → détection → PostgreSQL et topics d'alertes
  db.py                schéma et écritures PostgreSQL
  api/app.py           API FastAPI et service de l'interface
web/                   interface (HTML, CSS, JS, Leaflet)
notebooks/             analyse et modèles
tests/                 tests unitaires
docs/figures/          figures exportées pour la présentation
docker-compose.yml     stack complète
```

---

## Limites et pistes

- **Couverture** : sans clé, Digitraffic couvre la mer Baltique. Pour le trafic mondial, il faut
  utiliser `AIS_SOURCE=aisstream`.
- **ETA déclarées** : souvent non mises à jour par les équipages, ce qui explique une partie des
  « retards » observés. L'écart reste un bon indicateur à surveiller.
- **Routes** : la distance restante est une orthodromie corrigée d'un facteur 1,25. Un graphe de
  routes maritimes (ex. *searoute*) affinerait l'ETA.
- **Passage à l'échelle** : le processeur Python suffit pour quelques milliers de navires. Pour un
  flux mondial complet, on pourrait le remplacer par **Spark Structured Streaming** ou **Kafka
  Streams**, stocker l'historique dans **TimescaleDB** ou **Cassandra**, et orchestrer les analyses
  avec **Airflow**.

---

Données AIS © Fintraffic / Digitraffic, licence CC BY 4.0.
