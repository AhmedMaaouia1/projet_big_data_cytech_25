# RAPPORT DE PROJET BIG DATA
## Pipeline de Traitement des Données NYC Taxi

---

**Auteur** : Ahmed Maaouia  
**Lien GitHub** : https://github.com/AhmedMaaouia1/nyc-taxi-bigdata-pipeline

---

## TABLE DES MATIÈRES

1. [Introduction](#1-introduction)
2. [Architecture Globale](#2-architecture-globale)
3. [Exercice 1 - Data Retrieval](#3-exercice-1---data-retrieval)
4. [Exercice 2 - Data Ingestion](#4-exercice-2---data-ingestion)
5. [Exercice 3 - Data Warehouse](#5-exercice-3---data-warehouse)
6. [Exercice 4 - Dashboard & EDA](#6-exercice-4---dashboard--eda)
7. [Exercice 5 - ML Prediction Service](#7-exercice-5---ml-prediction-service)
8. [Exercice 6 - Orchestration Airflow](#8-exercice-6---orchestration-airflow)
9. [Difficultés Rencontrées et Solutions](#9-difficultés-rencontrées-et-solutions)
10. [Conclusion et Perspectives](#10-conclusion-et-perspectives)
11. [Annexes](#11-annexes)

---

## 1. INTRODUCTION

### 1.1 Contexte

Ce projet s'inscrit dans le cadre du cours de Big Data (année universitaire 2024-2025) et vise à mettre en pratique les concepts de traitement de données massives à travers un cas concret : l'analyse des courses de taxi à New York City.

Le dataset utilisé provient du NYC Taxi & Limousine Commission (TLC) et contient les données des courses de taxis jaunes (Yellow Cab) de Manhattan et des autres boroughs de New York. Chaque mois représente environ **3 millions de courses**, soit plus de **36 millions de lignes par an**.

### 1.2 Objectifs du Projet

Le projet vise à construire un pipeline Big Data complet, de bout en bout, capable de :

1. **Collecter** les données mensuellement depuis la source officielle NYC TLC
2. **Nettoyer et transformer** les données avec Apache Spark
3. **Stocker** les données dans un Data Warehouse relationnel
4. **Visualiser** les données via un Dashboard interactif
5. **Prédire** le prix des courses avec un modèle de Machine Learning
6. **Orchestrer** l'ensemble du pipeline avec Apache Airflow

### 1.3 Stack Technique

| Composant | Technologie | Version | Justification |
|-----------|-------------|---------|---------------|
| Traitement distribué | Apache Spark | 3.5.x | Standard industrie pour le Big Data |
| Langage EX01/EX02 | Scala | 2.12 | Performance et typage fort |
| Langage EX05 | Python/PySpark | 3.10 | Écosystème ML riche |
| Data Lake | MinIO | Latest | Compatible S3, gratuit, déployable localement |
| Data Warehouse | PostgreSQL | 15 | SQL standard, robuste, Open Source |
| Machine Learning | PySpark MLlib | 3.5.x | Intégré à Spark, scalable |
| Dashboard | Streamlit | Latest | Développement rapide, interactif |
| Orchestration | Apache Airflow | 2.8.1 | Standard industrie, backfill natif |
| Conteneurisation | Docker Compose | Latest | Reproductibilité, isolation |

---

## 2. ARCHITECTURE GLOBALE

### 2.1 Vue d'Ensemble

L'architecture du projet suit le pattern **Lakehouse**, combinant les avantages d'un Data Lake (stockage flexible, scalable) et d'un Data Warehouse (requêtes SQL, schéma structuré).

<!-- CAPTURE_ARCHITECTURE : Insérer ici Documents/Project_Architecture.png -->
**[CAPTURE À INSÉRER : Documents/Project_Architecture.png - Vue globale de l'architecture]**

### 2.2 Infrastructure Docker

L'ensemble de l'infrastructure est déployée via Docker Compose, garantissant la reproductibilité et l'isolation des composants.

| Conteneur | Rôle | Port |
|-----------|------|------|
| spark-master | Coordinateur Spark | 8081, 7077 |
| spark-worker-1 | Exécuteur Spark | 8082 |
| spark-worker-2 | Exécuteur Spark | 8083 |
| minio | Data Lake S3 | 9000, 9001 |
| postgres | Data Warehouse | 5432 |
| airflow-webserver | UI Airflow | 8080 |
| airflow-scheduler | Orchestrateur | - |
| streamlit | Dashboard | 8501 |

Tous les conteneurs partagent le réseau Docker `nyc-net`, permettant la communication inter-services.

### 2.3 Architecture Data Lake (MinIO)

Le Data Lake MinIO est organisé en **3 zones** suivant le pattern Medallion :

| Bucket | Zone | Statut | Description |
|--------|------|--------|-------------|
| `nyc-raw` | Bronze (Raw Zone) | ✅ Utilisé | Données brutes téléchargées depuis NYC TLC |
| `nyc-interim` | Silver (Interim Zone) | ✅ Utilisé | Données nettoyées et transformées par EX02 |
| `nyc-processed` | Gold (Curated Zone) | 🔮 **Prévu** | Zone réservée pour évolutions futures |

#### Zone `nyc-processed` : Évolutions Futures Prévues

La zone **Gold (`nyc-processed`)** est provisionnée mais non utilisée dans la version actuelle. Elle est destinée à accueillir :

| Évolution | Description | Cas d'usage |
|-----------|-------------|-------------|
| **Prédictions ML** | Stockage des prédictions de prix générées par le modèle | Batch scoring mensuel, export vers BI |
| **Agrégations** | Données pré-agrégées par zone, heure, jour | Dashboard temps réel, KPIs métier |
| **Features Store** | Features pré-calculées pour le ML | Réutilisation cross-modèles, cohérence |
| **Data Products** | Datasets prêts à consommer pour les utilisateurs finaux | Self-service analytics, API data |

Cette architecture en 3 zones permet une **évolution progressive** sans refonte majeure.

### 2.4 Flux de Données

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         FLUX DE DONNÉES                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│   NYC TLC Website                                                        │
│        │                                                                 │
│        ▼                                                                 │
│   ┌─────────┐     ┌─────────────────────────────────────┐               │
│   │  EX01   │────►│  MinIO: nyc-raw/yellow/YYYY/MM/     │  BRONZE       │
│   │ Retrieval│     │  (Données brutes Parquet)          │               │
│   └─────────┘     └─────────────────────────────────────┘               │
│        │                                                                 │
│        ▼                                                                 │
│   ┌─────────┐     ┌─────────────────────────────────────┐               │
│   │  EX02   │────►│  MinIO: nyc-interim/yellow/YYYY/MM/ │  SILVER       │
│   │Ingestion│     │  (Données nettoyées Parquet)        │               │
│   └─────────┘     └─────────────────────────────────────┘               │
│        │                                                                 │
│        ├─────────►┌─────────────────────────────────────┐               │
│        │          │  PostgreSQL: yellow_trips_staging   │               │
│        │          │  (Table staging temporaire)         │               │
│        │          └─────────────────────────────────────┘               │
│        │                         │                                       │
│        │                         ▼                                       │
│        │          ┌─────────────────────────────────────┐               │
│        │          │  EX03: Data Warehouse               │               │
│        │          │  - fact_trip                        │               │
│        │          │  - dim_date, dim_time               │               │
│        │          │  - dim_location, dim_vendor         │               │
│        │          └─────────────────────────────────────┘               │
│        │                                                                 │
│        ▼                                                                 │
│   ┌─────────┐     ┌─────────────────────────────────────┐               │
│   │  EX05   │────►│  Model Registry (fichiers locaux)   │               │
│   │   ML    │     │  - candidate_model/                 │               │
│   └─────────┘     │  - current_model/                   │               │
│                   └─────────────────────────────────────┘               │
│        │                                                                 │
│        ▼ (FUTUR)                                                        │
│   ┌─────────────────────────────────────────────────────┐               │
│   │  MinIO: nyc-processed/                              │  GOLD         │
│   │  (Prédictions, agrégations, features store)         │  (PRÉVU)      │
│   └─────────────────────────────────────────────────────┘               │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 2.5 Choix d'Architecture

| Décision | Alternative considérée | Justification du choix |
|----------|------------------------|------------------------|
| MinIO vs HDFS | HDFS | MinIO est plus léger, compatible S3, facile à déployer |
| PostgreSQL vs Hive | Hive | PostgreSQL est plus rapide pour les requêtes analytiques simples |
| Airflow vs Prefect | Prefect | Airflow est le standard industrie, meilleur pour le CV |
| LocalExecutor vs Celery | CeleryExecutor | Suffisant pour le volume du projet, plus simple |
| 3 zones Data Lake | 2 zones | Prépare l'évolution future (Gold zone prête) |

---

## 3. EXERCICE 1 - DATA RETRIEVAL

### 3.1 Objectif

Télécharger automatiquement les fichiers Parquet mensuels depuis le site NYC TLC et les stocker dans le Data Lake MinIO (zone Raw/Bronze).

### 3.2 Implémentation

**Langage** : Scala 2.12  
**Fichier principal** : `ex01_data_retrieval/src/main/scala/Ex01DataRetrieval.scala`

Le job Spark accepte deux paramètres :
- `--year` : Année (ex: 2023)
- `--month` : Mois (ex: 01)

**Flux d'exécution** :
1. Vérification si le fichier existe déjà localement (idempotence)
2. Téléchargement depuis `https://d37ci6vzurychx.cloudfront.net/trip-data/`
3. Lecture du fichier Parquet avec Spark
4. Écriture vers MinIO (`s3a://nyc-raw/yellow/YYYY/MM/`)

### 3.3 Idempotence

L'idempotence est garantie par deux mécanismes :
1. **Skip du téléchargement** si le fichier existe déjà localement
2. **Mode overwrite** sur MinIO pour écraser les données existantes

```scala
df.write
  .mode("overwrite")
  .parquet(s3TargetPath)  // s3a://nyc-raw/yellow/2023/01/
```

> ⚠️ **Précision importante** : L'overwrite est **limité à la partition mensuelle `YYYY/MM/`** (ex: `nyc-raw/yellow/2023/01/`), et non à tout le bucket. Ainsi, réécrire janvier 2023 n'affecte pas les autres mois. Chaque mois est une partition indépendante.

### 3.4 Commande d'Exécution

```bash
docker exec spark-master spark-submit \
    --class Ex01DataRetrieval \
    --master spark://spark-master:7077 \
    /opt/workdir/ex01_data_retrieval/target/scala-2.12/ex01-data-retrieval_2.12-0.1.0.jar \
    --year 2023 --month 01
```

### 3.5 Capture d'Écran

<!-- CAPTURE_EX01_MINIO : Console MinIO montrant nyc-raw/yellow/2023/01/ avec les fichiers parquet -->
**[CAPTURE À INSÉRER : Console MinIO → Bucket nyc-raw → yellow/2023/01/ avec les fichiers parquet listés]**

---

## 4. EXERCICE 2 - DATA INGESTION

### 4.1 Objectif

Nettoyer les données brutes et les écrire vers deux destinations :
- **Branch 1** : MinIO (zone Interim/Silver) - Pour le ML
- **Branch 2** : PostgreSQL (table staging) - Pour le Data Warehouse

### 4.2 Architecture Double Branche

```
                    ┌────────────────────────────────────┐
                    │          EX02 Spark Job            │
                    │                                    │
   nyc-raw ────────►│  1. Lecture Parquet                │
   (Bronze)         │  2. Nettoyage & Filtrage           │
                    │  3. Sélection colonnes             │
                    │                                    │
                    │         ┌─────────────┐            │
                    │         │   Branch 1  │───────────►│ MinIO nyc-interim (Silver)
                    │         └─────────────┘            │
                    │         ┌─────────────┐            │
                    │         │   Branch 2  │───────────►│ PostgreSQL (staging)
                    │         └─────────────┘            │
                    └────────────────────────────────────┘
```

### 4.3 Transformations Appliquées

| Transformation | Description |
|----------------|-------------|
| Filtrage nulls | Suppression des lignes avec colonnes critiques nulles |
| Filtrage valeurs | `fare_amount > 0`, `trip_distance > 0`, `passenger_count > 0` |
| Sélection colonnes | 18 colonnes retenues sur 19 originales |
| Cast types | Conversion des types pour compatibilité PostgreSQL |

### 4.4 Idempotence

| Branche | Mécanisme | Scope |
|---------|-----------|-------|
| MinIO | `mode("overwrite")` | Partition mensuelle `YYYY/MM/` uniquement |
| PostgreSQL | `mode("overwrite")` + `option("truncate", "true")` | Table entière |

> 💡 **Justification du TRUNCATE sur staging** : La table `yellow_trips_staging` est **volontairement technique et rejouable**. Elle ne sert qu'au chargement analytique vers le Data Warehouse (EX03) et n'a pas vocation à historiser les données. Reconstruire le staging à chaque run mensuel garantit la cohérence et simplifie le debug.

### 4.5 Captures d'Écran

<!-- CAPTURE_EX02_MINIO : Console MinIO montrant nyc-interim/yellow/2023/01/ -->
**[CAPTURE À INSÉRER : Console MinIO → Bucket nyc-interim → yellow/2023/01/]**

<!-- CAPTURE_EX02_STAGING : pgAdmin/DBeaver montrant SELECT COUNT(*) FROM yellow_trips_staging -->
**[CAPTURE À INSÉRER : pgAdmin ou DBeaver → Requête SELECT COUNT(*) FROM yellow_trips_staging avec résultat]**

---

## 5. EXERCICE 3 - DATA WAREHOUSE

### 5.1 Objectif

Construire un Data Warehouse en modèle étoile (Star Schema) et charger les données depuis la table staging.

### 5.2 Modèle Dimensionnel

```
                              ┌─────────────────┐
                              │   dim_vendor    │
                              │─────────────────│
                              │ vendor_id (PK)  │
                              │ vendor_name     │
                              └────────┬────────┘
                                       │
┌─────────────────┐           ┌────────┴────────┐           ┌─────────────────┐
│    dim_date     │           │                 │           │  dim_location   │
│─────────────────│           │                 │           │─────────────────│
│ date_id (PK)    │◄──────────│   fact_trip     │──────────►│ location_id (PK)│
│ year            │           │                 │           │ borough         │
│ month           │           │─────────────────│           │ zone            │
│ day             │           │ trip_id (PK)    │           └─────────────────┘
│ day_of_week     │           │ pickup_date (FK)│
└─────────────────┘           │ pickup_time (FK)│           ┌─────────────────┐
                              │ vendor_id (FK)  │           │ dim_payment_type│
┌─────────────────┐           │ payment_type_id │──────────►│─────────────────│
│    dim_time     │           │ ratecode_id (FK)│           │ payment_type_id │
│─────────────────│           │ pickup_loc (FK) │           │ payment_name    │
│ time_id (PK)    │◄──────────│ dropoff_loc (FK)│           └─────────────────┘
│ hour            │           │ passenger_count │
│ minute          │           │ trip_distance   │           ┌─────────────────┐
└─────────────────┘           │ fare_amount     │           │  dim_ratecode   │
                              │ total_amount    │──────────►│─────────────────│
                              │ ...             │           │ ratecode_id (PK)│
                              └─────────────────┘           │ ratecode_name   │
                                                            └─────────────────┘
```

### 5.3 Tables Créées

| Table | Type | Lignes (exemple) | Description |
|-------|------|------------------|-------------|
| fact_trip | Fait | ~3,000,000/mois | Courses de taxi |
| dim_date | Dimension | ~365 | Dates uniques |
| dim_time | Dimension | ~1,440 | Minutes de la journée |
| dim_location | Dimension | ~265 | Zones NYC |
| dim_vendor | Dimension | 2 | Compagnies de taxi |
| dim_payment_type | Dimension | 5 | Modes de paiement |
| dim_ratecode | Dimension | 6 | Types de tarification |

### 5.4 Idempotence avec ON CONFLICT

```sql
INSERT INTO fact_trip (...)
SELECT ... FROM yellow_trips_staging
ON CONFLICT DO NOTHING;
```

> 🔑 **Clé de détection des doublons** : Les doublons sont évités via une **contrainte d'unicité composite** sur les colonnes `(pickup_date, pickup_time, pickup_location_id, dropoff_location_id, vendor_id)`. Cette combinaison identifie de manière unique une course. Le `ON CONFLICT DO NOTHING` ignore silencieusement les insertions de lignes déjà présentes, garantissant l'idempotence.

Le `ON CONFLICT DO NOTHING` garantit que :
- Les doublons ne sont pas insérés
- Le job peut être rejoué sans erreur
- Les données existantes sont préservées

### 5.5 Captures d'Écran

<!-- CAPTURE_EX03_FACT_COUNT : pgAdmin montrant SELECT COUNT(*) FROM fact_trip avec résultat -->
**[CAPTURE À INSÉRER : pgAdmin → Requête SELECT COUNT(*) FROM fact_trip avec le nombre de lignes]**

<!-- CAPTURE_EX03_SCHEMA : pgAdmin montrant la liste des tables (fact_trip + dim_*) -->
**[CAPTURE À INSÉRER : pgAdmin → Vue des tables du schéma avec fact_trip et toutes les dim_*]**

---

## 6. EXERCICE 4 - DASHBOARD & EDA

### 6.1 Objectif

Explorer les données via un notebook Jupyter et créer un Dashboard interactif avec Streamlit.

### 6.2 Analyses Exploratoires (EDA)

Le notebook `ex04_eda.ipynb` contient les analyses suivantes :

| Analyse | Insight principal |
|---------|-------------------|
| Distribution des prix | Médiane ~$15, queue longue vers les hauts montants |
| Corrélation distance/prix | Corrélation forte (~0.85) |
| Analyse temporelle | Pics à 8h et 18h (heures de pointe) |
| Zones fréquentées | Manhattan domine largement |
| Saisonnalité | Plus de courses en décembre |

### 6.3 Dashboard Streamlit

**URL d'accès** : http://localhost:8501

**Fonctionnalités du Dashboard** :
- Vue d'ensemble avec KPIs principaux
- Graphiques interactifs
- Filtres par date, zone, type de paiement
- Carte des zones NYC
- Analyse des tendances temporelles

> 📊 **Architecture du Dashboard** : Conformément aux contraintes du projet, **Plotly est utilisé uniquement pour l'affichage visuel**. Toutes les agrégations et calculs sont effectués **côté PostgreSQL via des requêtes SQL**. Le code Python ne fait que récupérer les résultats pré-agrégés et les afficher, sans transformation métier.

### 6.4 Captures d'Écran Dashboard

<!-- CAPTURE_EX04_DASHBOARD_HOME : Page d'accueil du Dashboard avec KPIs principaux -->
**[CAPTURE À INSÉRER : Dashboard Streamlit → Page d'accueil avec les KPIs (nombre courses, revenu total, distance moyenne)]**

<!-- CAPTURE_EX04_DASHBOARD_DISTRIBUTION : Graphique de distribution des prix -->
**[CAPTURE À INSÉRER : Dashboard Streamlit → Histogramme de distribution des prix (total_amount)]**

<!-- CAPTURE_EX04_DASHBOARD_MAP : Carte des zones ou heatmap -->
**[CAPTURE À INSÉRER : Dashboard Streamlit → Carte ou heatmap des zones les plus fréquentées]**

<!-- CAPTURE_EX04_DASHBOARD_TEMPORAL : Analyse temporelle (heures de pointe) -->
**[CAPTURE À INSÉRER : Dashboard Streamlit → Graphique des courses par heure de la journée]**

<!-- CAPTURE_EX04_DASHBOARD_FILTERS : Interface avec filtres actifs -->
**[CAPTURE À INSÉRER : Dashboard Streamlit → Vue avec filtres (date, zone, payment_type) appliqués]**

---

## 7. EXERCICE 5 - ML PREDICTION SERVICE

### 7.1 Objectif

Développer un service de prédiction du prix total d'une course (`total_amount`) en utilisant PySpark MLlib.

### 7.2 Stratégie de Fenêtre Glissante

Pour éviter le **data leakage** et simuler un environnement de production, nous utilisons une stratégie de **fenêtre glissante** (sliding window) :

```
Mois M (test)     : 2023-04
Mois training     : 2023-01, 2023-02, 2023-03  (3 mois précédents)

Mois M+1 (test)   : 2023-05
Mois training     : 2023-02, 2023-03, 2023-04  (fenêtre décalée)
```

### 7.3 Features Utilisées

| Feature | Type | Description | Transformation |
|---------|------|-------------|----------------|
| trip_distance | Numérique | Distance en miles | StandardScaler |
| passenger_count | Numérique | Nombre de passagers | - |
| hour_of_day | Numérique | Heure de prise en charge | Extrait de pickup_datetime |
| day_of_week | Numérique | Jour de la semaine (0-6) | Extrait de pickup_datetime |
| PULocationID | Catégoriel | Zone de départ | StringIndexer |
| DOLocationID | Catégoriel | Zone d'arrivée | StringIndexer |

### 7.4 Algorithme : Gradient Boosted Trees

Nous utilisons **GBTRegressor** de PySpark MLlib :

```python
gbt = GBTRegressor(
    featuresCol="features",
    labelCol="total_amount",
    maxIter=50,
    maxDepth=5,
    stepSize=0.1
)
```

**Justification** : GBT offre un bon compromis entre performance et interprétabilité, et gère bien les features numériques et catégorielles.

### 7.5 Model Registry

Le système de Model Registry gère automatiquement :

```
models/registry/
├── current_model/          # Modèle en production
│   ├── model/              # Fichiers du modèle Spark
│   └── metadata.json       # Métriques et infos
├── candidate_model/        # Nouveau modèle à évaluer
│   ├── model/
│   └── metadata.json
└── promotion_history.json  # Historique des promotions
```

**Règle de promotion** : Le candidat est promu si **au moins 2 métriques sur 3** s'améliorent :
- RMSE (doit diminuer)
- R² (doit augmenter)
- MAE (doit diminuer)

### 7.6 Résultats Obtenus

| Métrique | Valeur | Seuil projet | Statut |
|----------|--------|--------------|--------|
| **RMSE** | **5.17** | < 10 | ✅ Atteint |
| **R²** | **0.9423** | > 0.5 | ✅ Atteint |
| **MAE** | **2.05** | - | ✅ Excellent |

Ces résultats démontrent que le modèle prédit le prix d'une course avec une erreur moyenne de **$2.05** et explique **94.23%** de la variance des prix.

<!-- CAPTURE_EX05_METRICS : Fichier train_metrics.json ou sortie console montrant RMSE/MAE/R² -->
**[CAPTURE À INSÉRER : Contenu du fichier reports/train_metrics.json OU sortie console du training avec les métriques]**

<!-- CAPTURE_EX05_STREAMLIT_ML : Interface Streamlit ML Demo si disponible -->
**[CAPTURE À INSÉRER : Dashboard Streamlit ML Demo montrant les prédictions (optionnel)]**

### 7.7 Logging Structuré

Le module EX05 utilise un système de logging standardisé via le module `logging_config.py` :

```python
from logging_config import get_logger

logger = get_logger(__name__)
logger.info("Training started for month 2023-04")
```

Format des logs :
```
2026-01-09 14:30:00 | INFO     | ml_pipeline              | Training started for month 2023-04
```

---

## 8. EXERCICE 6 - ORCHESTRATION AIRFLOW

### 8.1 Objectif

Orchestrer le pipeline complet (EX01 → EX05) de manière mensuelle, idempotente et rattrapable (backfill).

### 8.2 Architecture Airflow

| Composant | Configuration |
|-----------|---------------|
| Executor | LocalExecutor |
| Base de données | PostgreSQL (airflow-postgres) |
| Schedule | `@monthly` |
| Start date | 2023-01-01 |
| End date | 2023-05-31 (5 mois) |
| Catchup | Activé (backfill possible) |

> 📅 **Note sur la période** : Le DAG est configuré avec `end_date=2023-05-31`, ce qui limite le traitement aux mois de **Janvier à Mai 2023** (5 mois). Cette limitation est volontaire pour les besoins du projet. Pour traiter d'autres mois, il suffit de modifier la valeur `end_date` dans le fichier `full_pipeline_dag.py`.

### 8.3 DAG Principal : `full_nyc_taxi_pipeline`

```
start_pipeline
      │
      ▼
log_pipeline_params
      │
      ▼
check_source_data_available ─────► (short-circuit si données indisponibles)
      │
      ▼
ex01_start → ex01_spark_submit → ex01_verify → ex01_complete
                                                      │
                                                      ▼
ex02_start → ex02_spark_submit ─┬─► ex02_verify_minio_interim ────┐
                                │                                  │
                                └─► ex02_verify_postgres_staging ──┼─► ex02_quality_check
                                                                   │           │
                                                                   ▼           ▼
                                                              ex02_complete
                                                                   │
                                    ┌──────────────────────────────┴──────────────────────────────┐
                                    │                                                              │
                                    ▼                                                              ▼
ex03_start → ex03_load_dimensions → ex03_load_fact_trip → ex03_verify → ex03_complete    ex05_check_can_run
                                                                              │                    │
                                                                              │                    ▼
                                                                              │           ex05_compute_ml_params
                                                                              │                    │
                                                                              │                    ▼
                                                                              │           ex05_start → ex05_run_ml → ex05_verify → ex05_complete
                                                                              │                                                          │
                                                                              └──────────────────────────────────────────────────────────┘
                                                                                                           │
                                                                                                           ▼
                                                                                                    pipeline_success
                                                                                                           │
                                                                                                           ▼
                                                                                                  log_pipeline_completion
```

### 8.4 Fonctionnalités Avancées

#### SLA (Service Level Agreement)

| Tâche | SLA | Justification |
|-------|-----|---------------|
| ex01_spark_submit | 30 min | Téléchargement + upload |
| ex02_spark_submit | 1h30 | Nettoyage de ~3M lignes |
| ex03_load_fact_trip | 1h | Insertion SQL |
| ex05_run_ml_pipeline | 2h30 | Training ML |

#### Quality Checks

Vérification du comptage inter-étapes avec seuils :
- **< 80%** : FAIL (perte de données critique)
- **80-90%** : WARNING (log mais continue)
- **> 90%** : OK

#### Idempotence par Exercice

| Exercice | Mécanisme | Scope |
|----------|-----------|-------|
| EX01 | Skip si fichier existe + overwrite MinIO | Partition `YYYY/MM/` |
| EX02 | Overwrite MinIO + TRUNCATE staging | Partition + table entière |
| EX03 | ON CONFLICT DO NOTHING | Clé composite |
| EX05 | Modèle candidat + promotion conditionnelle | Modèle |

### 8.5 Captures d'Écran Airflow

<!-- CAPTURE_EX06_DAG_LIST : Vue liste des DAGs dans Airflow avec full_nyc_taxi_pipeline visible -->
**[CAPTURE À INSÉRER : Airflow UI → Page DAGs montrant full_nyc_taxi_pipeline dans la liste]**

<!-- CAPTURE_EX06_DAG_GRAPH : Graph view du DAG full_nyc_taxi_pipeline montrant toutes les tâches -->
**[CAPTURE À INSÉRER : Airflow UI → Graph view du DAG avec toutes les tâches et dépendances visibles]**

<!-- CAPTURE_EX06_DAG_TREE : Tree view montrant l'historique d'exécution (runs successifs) -->
**[CAPTURE À INSÉRER : Airflow UI → Tree view ou Grid view montrant plusieurs exécutions (success/failed)]**

<!-- CAPTURE_EX06_TASK_LOG : Logs d'une tâche (ex: ex01_spark_submit ou ex05_run_ml) -->
**[CAPTURE À INSÉRER : Airflow UI → Logs d'une tâche montrant les messages de succès]**

---

## 9. DIFFICULTÉS RENCONTRÉES ET SOLUTIONS

### 9.1 Problèmes Techniques

| Problème | Symptôme | Solution |
|----------|----------|----------|
| Connexion Spark ↔ MinIO | `NoSuchBucket` ou `AccessDenied` | Configuration S3A avec `fs.s3a.endpoint` + credentials AWS |
| Timeout téléchargement | Job EX01 timeout après 30min | Retry avec délai exponentiel (3 retries, 2min delay) |
| Mémoire Spark insuffisante | `OutOfMemoryError` sur Worker | Ajustement `driver-memory=4g`, `executor-memory=4g` |
| Permissions Docker Windows | Erreurs de permissions Airflow | `AIRFLOW_UID=50000` dans le fichier `.env` |
| Réseau Docker | Conteneurs ne se voient pas | Création du réseau partagé `nyc-net` |

### 9.2 Choix de Conception

| Décision | Alternatives | Justification |
|----------|--------------|---------------|
| Scala pour EX01/EX02 | Python | Performance, typage fort, moins d'erreurs runtime |
| Python pour EX05 | Scala | Écosystème ML riche, facilité de développement |
| LocalExecutor | CeleryExecutor | Suffisant pour 1 DAG, pas besoin de Redis/RabbitMQ |
| ON CONFLICT DO NOTHING | UPSERT | Plus simple, les corrections de données sources sont rares |
| 1 DAG unique | Multi-DAGs | Vision globale, backfill simplifié, plus facile à expliquer |
| Staging TRUNCATE | Staging cumulatif | Simplicité, rejouabilité, pas besoin d'historiser |

### 9.3 Leçons Apprises

1. **Toujours tester en local** avant de déployer sur le cluster
2. **Les logs sont essentiels** - Passer de `print()` à `logging` a facilité le debug
3. **L'idempotence n'est pas optionnelle** - Chaque composant doit être rejouable
4. **Docker simplifie mais ajoute de la complexité réseau** - Bien comprendre les réseaux Docker
5. **Définir les clés d'unicité tôt** - Évite les problèmes de doublons en production

---

## 10. CONCLUSION ET PERSPECTIVES

### 10.1 Bilan du Projet

Ce projet a permis de construire un **pipeline Big Data complet et fonctionnel**, de la collecte des données jusqu'à la prédiction ML, en passant par le stockage et la visualisation.

**Réalisations principales** :
- ✅ 6 exercices complétés et fonctionnels
- ✅ Architecture distribuée avec Spark (1 master, 2 workers)
- ✅ Data Lake MinIO avec 3 zones (raw, interim, processed prévu)
- ✅ Data Warehouse PostgreSQL en modèle étoile
- ✅ Modèle ML avec **RMSE = 5.17** < 10 (objectif atteint)
- ✅ Orchestration Airflow avec backfill et SLA
- ✅ Infrastructure 100% containerisée et reproductible

### 10.2 Compétences Acquises

| Domaine | Compétences |
|---------|-------------|
| Big Data | Apache Spark, traitement distribué, partitionnement |
| Data Engineering | ETL, Data Lake, Data Warehouse, modélisation dimensionnelle |
| MLOps | Model Registry, fenêtre glissante, métriques de qualité |
| DevOps | Docker, Docker Compose, orchestration |
| Orchestration | Apache Airflow, DAGs, SLA, idempotence |

### 10.3 Améliorations Possibles et Zone `nyc-processed`

#### Évolutions Court Terme

| Priorité | Amélioration | Effort |
|----------|--------------|--------|
| Haute | Alerting Slack/Email en cas d'échec | 2h |
| Haute | Data drift detection pour le ML | 4h |
| Moyenne | Tests d'intégration automatisés | 4h |

#### Exploitation de la Zone Gold (`nyc-processed`)

La zone `nyc-processed` est provisionnée et prête à être utilisée pour les évolutions suivantes :

| Évolution | Description | Bénéfice |
|-----------|-------------|----------|
| **Batch Scoring** | Écriture des prédictions ML mensuelles vers `s3a://nyc-processed/predictions/` | Historique des prédictions, audit trail |
| **Agrégations pré-calculées** | KPIs par zone/heure/jour dans `s3a://nyc-processed/aggregates/` | Dashboard temps réel performant |
| **Features Store** | Features ML versionnées dans `s3a://nyc-processed/features/` | Réutilisation cross-modèles, cohérence |
| **Data Products** | Exports CSV/JSON pour utilisateurs métier dans `s3a://nyc-processed/exports/` | Self-service analytics |

**Exemple d'implémentation future** :
```python
# Après le training ML, sauvegarder les prédictions
predictions_df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("s3a://nyc-processed/predictions/")
```

#### Évolutions Long Terme

| Amélioration | Effort | Impact |
|--------------|--------|--------|
| Migration vers Kubernetes | 2 jours | Scalabilité cloud |
| Streaming avec Kafka | 3 jours | Temps réel |
| Interface Grafana | 1 jour | Monitoring avancé |
| API REST pour prédictions | 1 jour | Intégration applicative |

### 10.4 Perspectives

Ce projet constitue une base solide pour :
- Passer à l'échelle avec Kubernetes et Spark sur K8s
- Ajouter du streaming avec Kafka + Spark Structured Streaming
- Déployer en production sur le cloud (AWS EMR, GCP Dataproc, Azure Synapse)
- Exploiter pleinement l'architecture Medallion avec la zone Gold

---

## 11. ANNEXES

### A. Structure du Projet

```
nyc-taxi-bigdata-pipeline/
├── docker-compose.yml          # Infrastructure principale
├── .env                        # Variables d'environnement
├── README.md                   # Documentation principale
│
├── Docker/                     # Fichiers Docker
│   ├── Dockerfile              # Image Spark
│   └── Dockerfile.streamlit    # Image Streamlit
│
├── Documents/                  # Documentation et diagrammes
│   ├── Project_Architecture.png
│   └── RAPPORT_PROJET_BIGDATA.pdf
│
├── ex01_data_retrieval/        # EX01 - Collecte données
│   ├── src/main/scala/
│   └── build.sbt
│
├── ex02_data_ingestion/        # EX02 - Nettoyage et ingestion
│   ├── src/main/scala/
│   └── build.sbt
│
├── ex03_sql_table_creation/    # EX03 - Data Warehouse
│   ├── dw_creation.sql
│   └── dw_load_incremental.sql
│
├── ex04_dashboard/             # EX04 - Visualisation
│   ├── notebooks/
│   └── streamlit_app/
│
├── ex05_ml_prediction_service/ # EX05 - Machine Learning
│   ├── src/
│   │   ├── ml_pipeline.py
│   │   ├── trainer.py
│   │   ├── logging_config.py   # Logging standardisé
│   │   └── ...
│   ├── models/
│   ├── tests/
│   └── reports/
│
└── ex06_airflow/               # EX06 - Orchestration
    ├── dags/
    │   ├── full_pipeline_dag.py  # DAG principal
    │   └── ...
    ├── tests/
    │   └── test_dags.py
    ├── docker-compose.yml
    └── README.md
```

### B. Commandes Utiles

```bash
# Démarrer l'infrastructure principale
docker-compose up -d

# Vérifier les services
docker-compose ps

# Démarrer Airflow
cd ex06_airflow
docker-compose up -d

# Accéder aux interfaces
# Spark Master UI  : http://localhost:8081
# MinIO Console    : http://localhost:9001 (minioadmin/minioadmin123)
# Airflow UI       : http://localhost:8080 (airflow/airflow)
# Streamlit        : http://localhost:8501
# PostgreSQL       : localhost:5432 (postgres/postgres)

# Lancer un job Spark manuellement
docker exec spark-master spark-submit \
    --class Ex01DataRetrieval \
    --master spark://spark-master:7077 \
    /opt/workdir/ex01_data_retrieval/target/scala-2.12/ex01-data-retrieval_2.12-0.1.0.jar \
    --year 2023 --month 01

# Lancer le pipeline ML
docker exec spark-master spark-submit \
    --master spark://spark-master:7077 \
    /opt/workdir/ex05_ml_prediction_service/src/ml_pipeline.py \
    --test-month 2023-04 \
    --train-months 2023-01,2023-02,2023-03

# Consulter les logs Airflow
docker-compose logs -f airflow-scheduler

# Exécuter les tests DAG
cd ex06_airflow
pytest tests/ -v

# Arrêter tout
docker-compose down
cd ex06_airflow && docker-compose down
```

### C. Variables d'Environnement

```bash
# .env (racine du projet)
PROJECT_ROOT=S:/PROJECTS/projects/nyc-taxi-bigdata-pipeline
DOCKER_VOLUMES_ROOT=S:/dockervolumes

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123

POSTGRES_DB=nyc_taxi
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

### D. Buckets MinIO

| Bucket | Zone | Statut | Contenu |
|--------|------|--------|---------|
| `nyc-raw` | Bronze | ✅ Actif | Parquet bruts téléchargés |
| `nyc-interim` | Silver | ✅ Actif | Parquet nettoyés pour ML |
| `nyc-processed` | Gold | 🔮 Prévu | Prédictions, agrégations (futur) |

### E. Liste des Captures d'Écran à Insérer

| ID | Section | Description |
|----|---------|-------------|
| CAPTURE_ARCHITECTURE | 2.1 | Documents/Project_Architecture.png |
| CAPTURE_EX01_MINIO | 3.5 | MinIO → nyc-raw/yellow/2023/01/ |
| CAPTURE_EX02_MINIO | 4.5 | MinIO → nyc-interim/yellow/2023/01/ |
| CAPTURE_EX02_STAGING | 4.5 | pgAdmin → COUNT(*) FROM staging |
| CAPTURE_EX03_FACT_COUNT | 5.5 | pgAdmin → COUNT(*) FROM fact_trip |
| CAPTURE_EX03_SCHEMA | 5.5 | pgAdmin → Liste des tables |
| CAPTURE_EX04_DASHBOARD_HOME | 6.4 | Streamlit → Page accueil + KPIs |
| CAPTURE_EX04_DASHBOARD_DISTRIBUTION | 6.4 | Streamlit → Histogramme prix |
| CAPTURE_EX04_DASHBOARD_MAP | 6.4 | Streamlit → Carte/Heatmap zones |
| CAPTURE_EX04_DASHBOARD_TEMPORAL | 6.4 | Streamlit → Graphique par heure |
| CAPTURE_EX04_DASHBOARD_FILTERS | 6.4 | Streamlit → Vue avec filtres |
| CAPTURE_EX05_METRICS | 7.6 | train_metrics.json ou console |
| CAPTURE_EX05_STREAMLIT_ML | 7.6 | Streamlit ML Demo (optionnel) |
| CAPTURE_EX06_DAG_LIST | 8.5 | Airflow → Liste DAGs |
| CAPTURE_EX06_DAG_GRAPH | 8.5 | Airflow → Graph view |
| CAPTURE_EX06_DAG_TREE | 8.5 | Airflow → Tree/Grid view |
| CAPTURE_EX06_TASK_LOG | 8.5 | Airflow → Logs d'une tâche |

### F. Références

- NYC TLC Trip Record Data : https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- Apache Spark Documentation : https://spark.apache.org/docs/latest/
- Apache Airflow Documentation : https://airflow.apache.org/docs/
- MinIO Documentation : https://min.io/docs/
- PySpark MLlib Guide : https://spark.apache.org/docs/latest/ml-guide.html
- Medallion Architecture : https://www.databricks.com/glossary/medallion-architecture

