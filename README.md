# Chicago Taxi Trips — Pipeline data medallion (Airflow + PySpark + MinIO)

Pipeline de bout en bout : ingestion de trajets de taxi bruts, nettoyage,
et agregation en KPIs prets pour la BI. Architecture medaillon
(bronze / silver / gold), orchestree par Airflow, processing en PySpark,
**stockage objet MinIO** comme socle du datalake.

## 1. Prerequis

- Docker + Docker Compose (v2)
- ~4 Go de RAM disponibles pour Docker
- Une connexion internet (pour l'appel a l'API SODA lors du premier run)

## 2. Lancement

```bash
# 1. Cloner le repo puis se placer a la racine

# 2. (optionnel) ajuster la periode telechargee dans .env
#    SODA_START_DATE / SODA_END_DATE (par defaut : 1er janvier 2023 - 30 juin 2023 inclus)

# 3. Build + demarrage de tous les services
#    Le DAG se declenche automatiquement (service airflow-trigger) une fois
#    qu'Airflow est pret : aucune action manuelle necessaire.
docker compose up -d --build

# 4. Suivre l'execution dans l'UI Airflow : http://localhost:8080
#    (login: admin / admin)

# 5. Explorer les donnees dans la console MinIO : http://localhost:9001
#    (login: minioadmin / minioadmin, ou les valeurs de .env)
#    Bucket "taxi-datalake", dossiers bronze/ silver/ gold/
```

Pour tout arreter : `docker compose down` (ajouter `-v` pour repartir de
zero, y compris les donnees MinIO et la base metadata d'Airflow).

## 3. Architecture

```
┌────────────┐   API SODA    ┌─────────────┐   Spark    ┌─────────────┐   Spark    ┌─────────────┐
│  Chicago   │ ──jour par──► │   BRONZE    │ ─parsing──►│   SILVER    │ ─agrege───►│    GOLD     │
│  Taxi API  │   jour (JSON) │ (raw parquet│  typage    │  (cleaned   │  jointures │ (KPIs prets │
│  (wrvz-psew)│  landing zone│ partitionne)│  dedup     │  parquet)   │  metier    │  pour la BI)│
│             │  disque local│             │            │             │            │             │
└────────────┘               └──────┬──────┘            └──────┬──────┘            └──────┬──────┘
                                     │                          │                          │
                                     └──────────────┬───────────┴──────────────────────────┘
                                                     ▼
                                    ┌──────────────────────────────────────┐
                                    │   MinIO (stockage objet S3-compatible) │
                                    │   bucket taxi-datalake/                │
                                    │     bronze/trips/ silver/trips/        │
                                    │     gold/daily_revenue/ ...            │
                                    │   Console web : localhost:9001         │
                                    └──────────────────────────────────────┘
```

Orchestration : un DAG Airflow unique `chicago_taxi_medallion_pipeline`
avec 4 taches sequentielles :
`download_raw_data >> ingest_bronze >> clean_silver >> aggregate_gold`.

Seule l'ingestion API (`download_raw_data`) ecrit sur le disque local
(`data/raw/`), en simple *landing zone* transitoire pour le JSON brut
telecharge page par page. Toutes les couches du datalake proprement dites
(bronze/silver/gold) sont ecrites par Spark **directement sur MinIO**, via
le connecteur S3A, sous forme de fichiers Parquet partitionnes par mois
(colonne `year_month`).

**Exposition** : sans base SQL dediee, les fichiers gold (Parquet + CSV)
sont directement consultables par n'importe quel outil sachant lire du S3 :
DuckDB, pandas + s3fs, Trino, Superset, ou simplement en telechargeant les
CSV depuis la console MinIO.

## 4. Jeu de donnees

Dataset public **Chicago Taxi Trips** :
<https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew>

Le fichier complet fait plusieurs dizaines de Go : on ne le telecharge
jamais en entier. Le script `src/ingestion/download_chicago_taxi.py`
interroge l'API SODA (Socrata, via `sodapy`) **jour par jour** sur la
periode demandee : pour chaque jour, une requete `$where` simple (plage
horaire d'une seule journee), avec pagination `$offset` a l'interieur de
cette journee si le volume le necessite.

Ce decoupage quotidien est un choix deliberement different d'un curseur
compose sur toute la periode (`$where` avec `OR` sur (`timestamp`, `id`)) :
ce dernier s'est revele extremement instable cote Socrata sur ce dataset
(timeouts systematiques passe la 1ere page). Des clauses `$where` simples,
sur une plage courte, restent rapides et fiables quelle que soit la duree
totale de la periode demandee.

**Periode retenue par defaut : 1er janvier 2023 - 30 juin 2023 inclus** (configurable via
`.env`, variables `SODA_START_DATE` / `SODA_END_DATE`, `SODA_END_DATE`
etant exclue).

## 5. Choix techniques et arbitrages

- **MinIO comme stockage objet** : S3-compatible, facile a lancer en local
  via Docker Compose, et illustre le meme pattern qu'un vrai datalake cloud
  (S3/GCS/ADLS) sans dependre d'un compte cloud. Spark y ecrit via le
  connecteur S3A (jars `hadoop-aws` + `aws-java-sdk-bundle`, ajoutes a
  l'image dans le `Dockerfile`).
- **Spark en mode `local[*]`** plutot qu'un cluster Spark dedie : simplifie
  le `docker-compose` tout en restant du PySpark "pour de vrai" (memes
  APIs, meme moteur). Le code des jobs reste portable vers un vrai cluster
  (yarn/k8s) sans modification — seul `.master()` dans `spark_session.py`
  changerait.
- **Landing zone locale pour le JSON brut** : le telechargement API ecrit
  sur le disque local (`data/raw/`) plutot que directement sur MinIO. C'est
  une zone transitoire (pas une des couches du medaillon), lue par le job
  bronze puis jetable ; la simplicite (pas besoin de client S3 cote script
  Python) l'emporte ici sur la purete architecturale.
- **Partitionnement mensuel (`year_month`)** : colonne derivee de
  `trip_start_timestamp`, calculee dans Spark et utilisee pour partitionner
  les fichiers Parquet sur MinIO (un dossier par mois). Permet un
  requetage efficace par periode sans avoir a lire tout le dataset.
- **Pas de couche SQL/Postgres d'exposition** : les tables gold restent en
  Parquet/CSV sur MinIO. Plus simple, et suffisant pour ce scope — un vrai
  besoin d'interrogation SQL interactif justifierait d'ajouter un moteur
  de requetage (Trino, DuckDB, Athena-like) par-dessus ces fichiers plutot
  que de dupliquer les donnees dans une base a part.
- **Dedup en silver, pas en bronze** : bronze doit rester le miroir fidele
  de la source (traçabilite), toute la logique de qualite (dedup, filtres,
  typage) est concentree en silver.
- **Sortie gold en Parquet + CSV** : Parquet pour la performance, CSV pour
  qu'un analyste BI puisse ouvrir directement les resultats (ex. depuis la
  console MinIO) sans outil specifique.

## 6. Tables gold produites

| Table | Description |
|---|---|
| `daily_revenue` | Nombre de trajets, revenu total, part fare/tips par jour |
| `avg_trip_duration` | Duree moyenne (minutes) et distance moyenne (miles) par jour |
| `top_pickup_zones` | Top 10 des zones de prise en charge par nombre de trajets et revenu |

## 7. Ce que je ferais avec plus de temps

- **Moteur de requetage sur MinIO** : brancher DuckDB ou Trino directement
  sur les fichiers Parquet gold pour offrir une interface SQL a l'equipe
  BI, sans dupliquer les donnees dans une base a part.
- **Data quality / observabilite** : Great Expectations (ou checks Spark
  custom) entre chaque couche, avec des metriques exposees (lignes
  rejetees, taux de nullite) plutot que des filtres silencieux.
- **Cluster Spark dedie** (master + workers) avec `SparkSubmitOperator`,
  pour illustrer la scalabilite horizontale du processing.
- **Idempotence / incrementalite** : ingestion incrementale (delta depuis
  le dernier `trip_start_timestamp` traite) plutot qu'un `overwrite`
  complet des couches a chaque run.
- **Landing zone sur MinIO egalement** : ecrire le JSON brut directement
  sur MinIO (via boto3) plutot que sur le disque local, pour une
  tracabilite complete du pipeline dans le meme stockage objet.
- **Tests** : tests unitaires sur les fonctions de nettoyage/agregation
  (pytest + fixtures Spark locales), tests d'integration sur le DAG complet.
- **Alerting** : notifications Airflow (email/Slack) en cas d'echec ou de
  volume anormalement bas en sortie d'une tache.
- **CI** : lint + tests automatiques sur chaque PR (GitHub Actions).

## 8. Structure du repo

```
.
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env                     # config (periode SODA, identifiants MinIO)
├── dags/
│   └── taxi_pipeline_dag.py
├── src/
│   ├── ingestion/download_chicago_taxi.py
│   ├── bronze/ingest_bronze.py
│   ├── silver/clean_silver.py
│   ├── gold/aggregate_gold.py
│   └── common/
│       ├── spark_session.py   # config Spark + connecteur S3A
│       └── storage.py         # chemins s3a:// + colonne year_month
└── data/raw/                 # landing zone locale (JSON brut), gitignored
```
