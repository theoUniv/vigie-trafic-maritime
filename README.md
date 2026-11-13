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
| *Bonus* : deuxième source open data | **Météo marine Open-Meteo** (vent, rafales, vagues, visibilité) collectée dans Kafka : elle explique les ralentissements de zone, s'affiche sur la carte et alimente le LLM |
| *Bonus* : IA générative open source | **Bulletin de situation** rédigé toutes les 15 min et **assistant conversationnel** qui interroge la base, avec un LLM à poids ouverts (Qwen 2.5) exécuté localement par **Ollama** |
| Analyse et modèle de prédiction (rappels) | Notebook `notebooks/analyse_trafic.ipynb` : data viz matplotlib, **prévision de position à 10 min** et **Isolation Forest** |
| Architecture Big Data (rappels) | **Kafka → Spark Structured Streaming** : un job PySpark agrège `ais.positions` par fenêtres de 5 min (trafic par zone et type de navire, fréquentation des ports) et écrit dans PostgreSQL |

---

## Architecture

```mermaid
flowchart LR
    subgraph Sources["Sources open data"]
        DT["Digitraffic AIS<br/>REST · Baltique"]
        AS["aisstream.io<br/>WebSocket · mondial"]
        PT["Gazetteer des ports<br/>12 000 UN/LOCODE"]
        OM["Open-Meteo<br/>vent · vagues"]
    end

    subgraph Ingestion
        P["producer<br/>collecte / 60 s"]
        WX["weather<br/>collecte / 90 min"]
    end

    subgraph Kafka["Apache Kafka (KRaft)"]
        T1[("ais.positions")]
        T2[("ais.static")]
        T3[("ais.alerts")]
        T4[("ais.congestion")]
        T5[("ais.briefings")]
        T6[("weather.marine")]
    end

    subgraph Traitement
        PR["processor<br/>état des navires<br/>congestion · ETA · anomalies"]
        SP["Spark Structured Streaming<br/>agrégats par fenêtre de 5 min"]
    end

    subgraph IA["IA open source (local)"]
        BR["briefer<br/>bulletin / 15 min"]
        OL["Ollama<br/>LLM qwen2.5:3b"]
    end

    DB[("PostgreSQL<br/>état temps réel<br/>+ historique")]
    API["API FastAPI"]
    WEB["Carte web<br/>Leaflet"]
    NB["Notebook<br/>analyse + modèles"]
    UI["Kafka UI"]

    DT --> P
    AS -.-> P
    P --> T1 & T2
    OM --> WX --> T6
    T1 & T2 & T6 --> PR
    PT --> PR
    PR --> T3 & T4
    PR --> DB
    T1 --> SP
    PT --> SP
    SP --> DB
    DB --> API --> WEB
    DB --> NB
    T3 & T4 --> BR
    BR <--> OL
    BR --> T5
    BR --> DB
    API <-->|"assistant<br/>appels d'outils"| OL
    Kafka --- UI
```

| Service | Rôle | Port |
|---|---|---|
| `kafka` | Broker Apache Kafka 3.8 en mode KRaft (sans ZooKeeper) | 9092 |
| `kafka-ui` | Exploration des topics et des messages | [8080](http://localhost:8080) |
| `producer` | Collecte AIS → Kafka | – |
| `weather` | Collecte Open-Meteo (vent, vagues) → Kafka `weather.marine`, toutes les 90 min | – |
| `processor` | Kafka → détection → PostgreSQL et topics d'alertes | – |
| `postgres` | Stockage de l'état courant, de l'historique et des alertes | 5433 |
| `api` | API REST et interface web | [8000](http://localhost:8000) |
| `ollama` | Serveur de LLM open source local (modèles à poids ouverts) | 11434 |
| `ollama-pull` | Télécharge le modèle au premier démarrage puis s'arrête | – |
| `briefer` | Kafka (alertes, zones) → bulletin rédigé par le LLM → `ais.briefings` et PostgreSQL | – |
| `spark` | Job PySpark Structured Streaming : Kafka → agrégats par fenêtre → PostgreSQL | [4040](http://localhost:4040) (Spark UI) |

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
| Météo marine (vent, rafales, visibilité, vagues, température de l'eau) | [Open-Meteo](https://open-meteo.com) : API Forecast et Marine | CC BY 4.0 |
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

## Météo marine : Open-Meteo

Une deuxième source open data, [Open-Meteo](https://open-meteo.com) (sans clé, CC BY 4.0), donne
le contexte météo des ralentissements. Une zone qui ralentit par vent de force 7 n'a pas la même
signification qu'une zone qui ralentit par mer calme.

- **Collecte** : le service `weather` interroge l'API Forecast (vent à 10 m, rafales, visibilité,
  temps) et l'API Marine (hauteur, direction et période des vagues, température de l'eau) sur une
  grille de 0,5° × 1° couvrant la Baltique. Au premier passage, les points à terre (sans vagues) sont
  écartés et la liste des 191 points en mer est mémorisée. Chaque relevé est publié sur le topic
  Kafka `weather.marine` ; le producteur AIS n'est pas modifié.
- **Quota** : chez Open-Meteo, un point compte comme un appel (gratuit : 600 par minute, 10 000 par
  jour). Le collecteur étale ses requêtes et relève la grille toutes les 90 min, soit environ
  6 100 appels par jour (`WEATHER_INTERVAL_MIN`, `WEATHER_STEP`, `WEATHER_BBOX`).
- **Traitement** : le processeur consomme `weather.marine`, stocke `weather_current` et
  `weather_history`, et rattache à chaque zone de congestion ou de ralentissement la météo du point
  le plus proche (120 km au plus). L'état de la mer est classé en 4 niveaux (calme, agitée,
  difficile, dangereuse) d'après l'échelle de Beaufort pour le vent et l'échelle de Douglas pour la
  mer. Par mer difficile, l'alerte de zone le dit : « … — météo : vent fort (27 nd, force 6) ».
- **Restitution** : couche « Météo marine » sur la carte (une flèche par point, orientée dans le sens
  du vent et colorée selon l'état de la mer), météo dans la liste des zones et dans la fiche navire.
  Le bulletin du LLM cite la météo comme cause probable quand la mer est difficile, et l'assistant a
  un outil `sea_conditions`.

| Niveau | Vent | Mer | Autres |
|---|---|---|---|
| calme | < 17 nd | < 1,25 m | |
| agitée | 17 à 21 nd | 1,25 à 2,5 m | |
| difficile | ≥ 22 nd (force 6) | ≥ 2,5 m (mer forte) | visibilité < 1 km, orage |
| dangereuse | ≥ 34 nd (coup de vent) ou rafales ≥ 44 nd | ≥ 4 m (très forte) | |

---

## IA générative open source (Ollama)

Vigie intègre un **LLM à poids ouverts exécuté localement** par [Ollama](https://ollama.com) :
aucune donnée ne quitte la machine, aucune clé ni service payant. Le modèle par défaut,
`qwen2.5:3b` (licence Qwen, ~2 Go), tourne sur CPU, écrit bien le français et sait appeler des
outils. On peut le changer avec `LLM_MODEL` (`llama3.2:3b`, `mistral:7b`, `qwen2.5:7b`…).

Au premier `docker compose up`, le service `ollama-pull` télécharge le modèle (quelques minutes).
Suivre l'avancement : `docker compose logs -f ollama-pull briefer`.

**Bulletin de situation (onglet « Bulletin »).** Le service `briefer` est un nouveau consommateur
Kafka, ajouté sans modifier le producteur ni le processeur. Il lit `ais.alerts` et
`ais.congestion` et, toutes les 15 minutes, condense la dernière heure en **faits structurés**
(indicateurs de flotte, alertes par gravité, zones saturées). Il fait ensuite rédiger un
bulletin en français par le LLM, publié sur `ais.briefings` et stocké en base. Le LLM ne reçoit que
des faits calculés par le pipeline et il a pour consigne de ne rien inventer. Si le LLM est
indisponible, un **bulletin gabarit** déterministe est produit à la place.

**Assistant (onglet « Assistant »).** `POST /api/ask` utilise l'**appel d'outils** (*tool calling*) :
le modèle ne connaît pas les données, il choisit parmi six outils (recherche de navires, fiche
navire, zones, alertes, statistiques, dernier bulletin) qui interrogent PostgreSQL, puis
formule sa réponse à partir des résultats. Les MMSI cités sont cliquables et ouvrent la fiche du navire.

```bash
curl -N -X POST localhost:8000/api/ask -H "Content-Type: application/json" \
     -d '{"question": "Quels cargos sont en retard vers Helsinki ?"}'
```

Pour désactiver l'IA : `LLM_URL=` (vide) dans `.env`, et ne pas lancer `ollama`.
Le reste de Vigie fonctionne normalement.

> Performances mesurées sur CPU (qwen2.5:3b) : environ 50 s par bulletin, et 20 à 30 s par question
> pour l'assistant. La réponse s'affiche au fil de l'eau : l'interface indique l'outil consulté, puis
> le texte s'écrit à mesure qu'il est généré. Le modèle reste en mémoire (`OLLAMA_KEEP_ALIVE=24h`) et
> l'API le précharge au démarrage. Avec une carte graphique NVIDIA, ajouter une réservation GPU au
> service `ollama` accélère nettement la génération. `qwen2.5:7b` choisit mieux ses outils, mais il
> est environ deux fois plus lent.

---

## Agrégats Big Data : Spark Structured Streaming

Le service `spark` exécute un job **PySpark Structured Streaming** (`maritime/spark/`) qui lit
directement le topic Kafka `ais.positions`. C'est un consommateur indépendant : il a ses propres
offsets, et ni le producteur ni le processeur n'ont été modifiés.

```mermaid
flowchart LR
    K[("Kafka<br/>ais.positions")] --> R["readStream<br/>JSON → positions typées"]
    R --> W["filigrane 10 min<br/>sur l'heure AIS"]
    V[("vessels<br/>JDBC")] --> J["jointure flux-statique<br/>type de navire"]
    W --> J
    J --> Z["fenêtres 5 min<br/>zone × type"]
    J --> C["fenêtres 5 min<br/>type"]
    W --> P["fenêtres 5 min<br/>port le plus proche"]
    Z & C & P --> S["foreachBatch<br/>upsert PostgreSQL"]
```

| Table | Clé | Contenu (par fenêtre de 5 min) |
|---|---|---|
| `spark_zone_traffic` | début de fenêtre, zone, type | Navires distincts, positions, navires à l'arrêt, vitesse moyenne et maximale, emprise de la zone |
| `spark_category_traffic` | début de fenêtre, type | Navires distincts, navires en route, vitesse moyenne (tous et en route) |
| `spark_port_traffic` | début de fenêtre, LOCODE | Fréquentation du port : navires présents dans un rayon de 5 nm, en mouvement ou à l'arrêt |

**Choix techniques :**

- **Temps de l'événement et filigrane.** Les fenêtres sont calculées sur l'horodatage AIS, pas
  sur l'heure de réception. Une position qui arrive jusqu'à 10 min après la fin de sa fenêtre est
  encore prise en compte. Ensuite, la fenêtre est figée et son état est libéré.
- **Dédoublonnage.** Kafka livre « au moins une fois », et le producteur republie l'instantané
  Digitraffic à chaque redémarrage. Les positions sont donc dédoublonnées sur (navire, horodatage
  AIS) avec `dropDuplicates`, un état lui aussi borné par le filigrane. Au redémarrage, l'instantané
  peut aussi apporter des positions réelles un peu anciennes que le processeur écarte (il ne garde
  que des trajectoires ordonnées). Spark les compte : seule la fenêtre du redémarrage a plus de
  positions, le nombre de navires ne change pas.
- **Mode `update` et upsert.** À chaque micro-lot (toutes les minutes), Spark ne renvoie que les
  fenêtres modifiées. Elles sont réécrites par `INSERT … ON CONFLICT DO UPDATE`.
- **Navires distincts** comptés avec `approx_count_distinct` (HyperLogLog++, erreur cible 1 %), car le comptage
  exact des valeurs distinctes n'est pas disponible en streaming.
- **Zones** : la même grille que le processeur, calculée en expressions Spark natives (sans UDF),
  avec des cases de 20 km (`SPARK_ZONE_KM`), mieux adaptées à une vue d'ensemble.
- **Ports** : une UDF réutilise l'index spatial du gazetteer UN/LOCODE (`PortIndex.nearest`).
- **Type de navire** : jointure avec la table `vessels`, relue à chaque micro-lot.
- **Reprise.** Chaque requête a un checkpoint (volume `spark-checkpoints`) qui contient les offsets
  Kafka et l'état des fenêtres. Au premier démarrage, le job relit l'historique du topic (24 h),
  par lots de 200 000 messages au plus.

Suivi : **Spark UI** sur http://localhost:4040 (onglet *Structured Streaming* : débit et durée des
micro-lots, état). Exemple de requête :

```sql
SELECT window_start, port_name, n_vessels, n_moving, n_stopped
FROM spark_port_traffic ORDER BY window_start DESC, n_vessels DESC LIMIT 10;
```

Paramètres (`.env`) : `SPARK_WINDOW`, `SPARK_WATERMARK`, `SPARK_TRIGGER`, `SPARK_ZONE_KM`,
`SPARK_PORT_RADIUS_NM`, `SPARK_DRIVER_MEMORY`, `SPARK_RETENTION_HOURS` (7 jours par défaut).

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
| `GET /api/weather` | Météo marine Open-Meteo sur la grille (vent, vagues, état de la mer) |
| `GET /api/briefings?limit=10` | Derniers bulletins de situation |
| `POST /api/ask` | Question en langage naturel à l'assistant (`{"question": "...", "history": [...]}`), réponse diffusée en NDJSON (`tool`, `token`, `done`) |
| `GET /api/llm/status` | État du LLM (activé, modèle téléchargé) |

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

Les 92 tests couvrent la géographie, le décodage AIS (ETA, pavillons, valeurs sentinelles), la
résolution des destinations, les détecteurs d'anomalies, la congestion et l'estimation d'ETA.
Ils couvrent aussi la couche IA (faits du bulletin, gabarit de repli, boucle d'appel d'outils),
testée avec un faux client, donc sans LLM, et la météo marine (lecture des réponses Open-Meteo,
état de la mer, rattachement aux zones).

`tests/test_spark.py` couvre le job Spark : lecture des messages, grille calculée par Spark identique
à celle de Python, agrégats par fenêtre, zone, type et port. Les tests qui demandent Spark sont
ignorés sans Java ; on les lance dans l'image Spark :

```bash
docker compose run --rm --no-deps -v ./tests:/app/tests spark python -m pytest -q tests
```

---

## Structure du dépôt

```
maritime/
  config.py            configuration (variables d'environnement)
  ais.py               référentiels AIS : types, statuts, pavillons (MID), ETA
  geo.py               distances, grille spatiale
  ports.py             gazetteer UN/LOCODE et résolution des destinations
  kafka_io.py          topics, producteur et consommateur JSON
  sources/             digitraffic.py (REST), aisstream.py (WebSocket), openmeteo.py (météo)
  weather.py           collecte Open-Meteo → Kafka (weather.marine)
  producer.py          collecte → Kafka
  processing/          state, anomalies, congestion, eta, weather (logique pure)
  processor.py         Kafka → détection → PostgreSQL et topics d'alertes
  db.py                schéma et écritures PostgreSQL
  llm/                 client Ollama, bulletin (faits + prompt), assistant (outils)
  briefer.py           Kafka → bulletin rédigé par le LLM → ais.briefings et PostgreSQL
  spark/               job Spark Structured Streaming (transformations, écriture, lancement)
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
- **Passage à l'échelle** : les agrégats tournent déjà sous **Spark Structured Streaming**, en mode
  local dans un conteneur. Pour un flux mondial complet, il suffirait de pointer `SPARK_MASTER`
  vers un cluster. La détection par navire (processeur Python) pourrait elle aussi migrer vers
  Spark (`flatMapGroupsWithState`) ou **Kafka Streams**. On pourrait aussi stocker l'historique
  dans **TimescaleDB** ou **Cassandra**, et orchestrer les analyses avec **Airflow**.

---

Données AIS © Fintraffic / Digitraffic, licence CC BY 4.0.
