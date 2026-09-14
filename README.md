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

## TL;DR
1. cloner repo et se mettre sur la branche main (par défaut)
2. se positionner sur le dossier du clone via terminal
3. lancer la commande: 
docker compose up -d --build
(si besoin de relancer: 
docker compose down (ou down -v) puis docker compose up -d --build)
4. Suivre l'execution dans l'UI Airflow : http://localhost:8080 (username: admin, password: admin)
5. Explorer les donnees dans la console MinIO : http://localhost:9001 (username: minioadmin, password: minioadmin)

**Attention** : `down -v` ne supprime que les volumes Docker (Postgres,
MinIO), pas `data/raw/` qui est un simple dossier local monté en volume
bind. Comme le pipeline fait un `overwrite` complet à chaque run (pas
d'ingestion incrémentale), il vaut mieux vider ce dossier avant de
relancer, pour ne pas retraiter d'anciens fichiers restants du run précédent. Pour cela lancer la commande (dossier du clone)

Remove-Item data\raw\*.json
​```

```bash
# 1. Cloner le repo puis se placer à la racine

# 2. (optionnel) ajuster la periode téléchargée dans .env
#    SODA_START_DATE / SODA_END_DATE (par defaut : 1er janvier 2023 - 31 mars 2023 inclus)

# 3. Build + démarrage de tous les services
#    Le DAG se déclenche automatiquement (service airflow-trigger) une fois
#    qu'Airflow est prêt : aucune action manuelle nécessaire.
docker compose up -d --build

# 4. Suivre l'exécution dans l'UI Airflow : http://localhost:8080
#    (login: admin / admin)

# 5. Explorer les données dans la console MinIO : http://localhost:9001
#    (login: minioadmin / minioadmin, ou les valeurs de .env)
#    Bucket "taxi-datalake", dossiers bronze/ silver/ gold/
```

Pour tout arrêter : `docker compose down` (ajouter `-v` pour repartir de
zéro, y compris les donnees MinIO et la base metadata d'Airflow).

## 3. Architecture

```
┌────────────┐   API SODA    ┌─────────────┐   Spark    ┌─────────────┐   Spark    ┌─────────────┐
│  Chicago   │ ──jour par──► │   BRONZE    │ ─parsing──►│   SILVER    │ ─agrégé───►│    GOLD     │
│  Taxi API  │   jour (JSON) │ (raw parquet│  typage    │  (cleaned   │  jointures │ (KPIs prets │
│  (wrvz-psew)│  landing zone│ partitionné)│  dédup     │  parquet)   │  métier    │  pour la BI)│
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
avec 4 tâches sequentielles :
`download_raw_data >> ingest_bronze >> clean_silver >> aggregate_gold`.

Seule l'ingestion API (`download_raw_data`) écrit sur le disque local
(`data/raw/`), en simple *landing zone* transitoire pour le JSON brut
téléchargé page par page. Toutes les couches du datalake proprement dites
(bronze/silver/gold) sont écrites par Spark **directement sur MinIO**, via
le connecteur S3A, sous forme de fichiers Parquet partitionnés par mois
(colonne `year_month`).

**Exposition** : sans base SQL dédiée, les fichiers gold (Parquet + CSV)
sont directement consultables par n'importe quel outil sachant lire du S3 :
DuckDB, pandas + s3fs, Trino, Superset, ou simplement en téléchargeant les
CSV depuis la console MinIO.

## 4. Jeu de donnees

Dataset public **Chicago Taxi Trips** :
<https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew>

Le fichier complet fait plusieurs dizaines de Go : on ne le télécharge
jamais en entier. Le script `src/ingestion/download_chicago_taxi.py`
interroge l'API SODA (Socrata, via `sodapy`) **jour par jour** sur la
periode demandée : pour chaque jour, une requête `$where` simple (plage
horaire d'une seule journée), avec pagination `$offset` à l'interieur de
cette journée si le volume le nécessite. Des clauses `$where` simples,
sur une plage courte, restent rapides et fiables quelle que soit la durée
totale de la période demandée.

**Période retenue par defaut : 1er janvier 2023 - 31 mars 2023 inclus** (configurable via
`.env`, variables `SODA_START_DATE` / `SODA_END_DATE`, `SODA_END_DATE`
étant exclue).

## 5. Choix techniques et arbitrages

- **MinIO comme stockage objet** : S3-compatible, facile à lancer en local
  via Docker Compose, et illustre le même pattern qu'un vrai datalake cloud
  (S3/GCS/ADLS) sans dépendre d'un compte cloud. Spark y écrit via le
  connecteur S3A (jars `hadoop-aws` + `aws-java-sdk-bundle`, ajoutés à
  l'image dans le `Dockerfile`).
- **Spark en mode `local[*]`** plutôt qu'un cluster Spark dédié. Le code des jobs reste portable vers un vrai cluster (yarn/k8s) sans modification — seul `.master()` dans `spark_session.py` changerait.
- **Landing zone locale pour le JSON brut** : le téléchargement API écrit
  sur le disque local (`data/raw/`) plutôt que directement sur MinIO. C'est
  une zone transitoire (pas une des couches du medaillon), lue par le job
  bronze, puis pas utilisée.
- **Partitionnement mensuel (`year_month`)** : colonne dérivée de
  `trip_start_timestamp`, calculée dans Spark et utilisée pour partitionner
  les fichiers Parquet sur MinIO (un dossier par mois). Permet un
  requêtage efficace par période sans avoir à lire tout le dataset.
- **Pas de couche SQL/Postgres d'exposition** : les tables gold restent en
  Parquet/CSV sur MinIO. Plus simple, et suffisant pour ce scope — un vrai
  besoin d'interrogation SQL interactif justifierait d'ajouter un moteur
  de requêtage (PostgreSQL ou DuckDB) par-dessus ces fichiers plutêt
  que de dupliquer les données dans une base a part. L'ajout de cette couche d'exposition est en cours, et peut être consulté via la branche : `feature_postgreSQL_BI_Layer`
- **Déduplication en silver, pas en bronze** : bronze doit rester le miroir fidèle
  de la source pour la traçabilité. La logique de qualite (dédoublonnage, filtres,
  typage) est implémentée en silver.
- **Sortie gold en Parquet + CSV** : Parquet pour la performance, CSV pour
  qu'un analyste BI puisse ouvrir directement les résultats en tant que fichiers plats. Sinon, la couche SQL pour l'exposition avec une connexion à un outil BI (PowerBI ou autre) aurait facilité le travail d'analyse.

## 6. Tables gold produites

| Table | Description |
|---|---|
| `daily_revenue` | Nombre de trajets, revenu total, part fare/tips par jour |
| `avg_trip_duration` | Duree moyenne (minutes) et distance moyenne (miles) par jour |
| `taxi_daily_performance` | performance quotidienne de chaque taxi: nombre de kms parcourus, temps effectué en trajet et montants facturés par jour par taxi|
| `company_daily_performance` | performance quotidienne de chaque compagnie de taxis: nombre de kms parcourus, temps effectué en trajet et montants facturés par jour par compagnie |
| `taxi_monthly_performance` | performance mensuelle de chaque taxi: nombre de kms parcourus, temps effectué en trajet et montants facturés par mois par taxi |
| `company_monthly_performance` | performance mensuelle de chaque compagnie: nombre de kms parcourus, temps effectué en trajet et montants facturés par jour par compagnie|

## 7. Ce que je ferais avec plus de temps

- **Moteur de requêtage sur MinIO** : brancher DuckDB ou PostreSQL directement
  sur les fichiers Parquet gold pour offrir une interface SQL à l'équipe
  BI, sans dupliquer les données dans une base a part.
- **Data quality / observabilité** : Great Expectations (ou checks Spark
  custom) entre chaque couche, avec des métriques exposées (lignes
  rejetées, taux de nullité) plutôt que des filtres silencieux.
- **Cluster Spark dédié** (master + workers) avec `SparkSubmitOperator`,
  pour illustrer la scalabilité horizontale du processing.
- **Idempotence / incrementalité** : ingestion incrementale (delta depuis
  le dernier `trip_start_timestamp` traite) plutot qu'un `overwrite`
  complet des couches a chaque run.
- **Landing zone sur MinIO egalement** : écrire le JSON brut directement
  sur MinIO (via boto3) plutôt que sur le disque local, pour une
  traçabilite complète du pipeline dans le même stockage objet.
- **Tests** : tests unitaires sur les fonctions de nettoyage/agrégation
  (pytest + fixtures Spark locales), tests d'intégration sur le DAG complet.
- **Alerting** : notifications Airflow (email/Slack) en cas d'échec ou de
  volume anormalement bas en sortie d'une tâche.
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
