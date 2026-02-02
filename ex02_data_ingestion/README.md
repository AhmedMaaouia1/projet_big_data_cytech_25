# Exercice 02 – Data Ingestion

## Objectif

L'exercice **EX02** réalise l'**ingestion et le nettoyage** des données brutes NYC Yellow Taxi, avec une architecture **double branche** :

1. **Branch 1** : Écriture vers le Data Lake (MinIO – `nyc-interim`)
2. **Branch 2** : Écriture vers la table staging PostgreSQL (Data Warehouse)

## Architecture

```
MinIO (nyc-raw)
      │
      ▼
┌─────────────────┐
│   Spark Job     │
│   EX02          │
│ (Nettoyage)     │
└─────────────────┘
      │
   ┌──┴──┐
   ▼     ▼
Branch 1  Branch 2
   │         │
   ▼         ▼
MinIO     PostgreSQL
(interim) (staging)
```

## Flux de données

```
s3a://nyc-raw/yellow/YYYY/MM/
         │
         ▼
    [Cleaning]
    - Cast types
    - Filtre temporel (mois strict)
    - Validation nulls
    - Cohérence générique
         │
    ┌────┴────┐
    ▼         ▼
s3a://nyc-interim/   yellow_trips_staging
yellow/YYYY/MM/      (PostgreSQL)
```

## Transformations appliquées

### Casting des types

| Colonne                  | Type cible   |
|--------------------------|--------------|
| VendorID                 | Integer      |
| tpep_pickup_datetime     | Timestamp    |
| tpep_dropoff_datetime    | Timestamp    |
| passenger_count          | Integer      |
| trip_distance            | Double       |
| RatecodeID               | Integer      |
| store_and_fwd_flag       | String       |
| PULocationID             | Integer      |
| DOLocationID             | Integer      |
| payment_type             | Integer      |
| fare_amount              | Double       |
| extra                    | Double       |
| mta_tax                  | Double       |
| tip_amount               | Double       |
| tolls_amount             | Double       |
| improvement_surcharge    | Double       |
| total_amount             | Double       |
| congestion_surcharge     | Double       |
| airport_fee              | Double       |

### Filtrage

- **Fenêtre temporelle stricte** : seuls les trajets dont le `pickup_datetime` est dans le mois traité sont conservés
- **Nulls critiques** : suppression des lignes avec pickup/dropoff datetime, PULocationID, DOLocationID null
- **Cohérence** : `trip_distance >= 0`, `total_amount >= 0`, `passenger_count >= 0` (ou null)

## Technologies

| Composant       | Version / Type    |
|-----------------|-------------------|
| Langage         | Scala 2.12        |
| Framework       | Apache Spark 3.5  |
| Build tool      | SBT               |
| Source Storage  | MinIO (S3A)       |
| Target Storage  | MinIO + PostgreSQL|

## Structure du projet

```
ex02_data_ingestion/
├── build.sbt
├── project/
│   └── build.properties
└── src/
    └── main/
        └── scala/
            └── Ex02DataIngestion.scala
```

## Paramètres CLI

| Paramètre     | Description                          | Défaut                      |
|---------------|--------------------------------------|-----------------------------|
| `--year`      | Année à traiter                      | 2023                        |
| `--month`     | Mois à traiter (01-12)               | 01                          |
| `--enableDw`  | Activer l'écriture PostgreSQL        | false                       |
| `--dwTable`   | Nom de la table staging              | public.yellow_trips_staging |

## Variables d'environnement requises

Pour la branche PostgreSQL (`--enableDw true`) :

| Variable           | Description                    |
|--------------------|--------------------------------|
| `POSTGRES_HOST`    | Hôte PostgreSQL (défaut: postgres) |
| `POSTGRES_PORT`    | Port PostgreSQL (défaut: 5432) |
| `POSTGRES_DB`      | Nom de la base                 |
| `POSTGRES_USER`    | Utilisateur                    |
| `POSTGRES_PASSWORD`| Mot de passe                   |

## Compilation

```bash
cd ex02_data_ingestion
sbt package
```

## Exécution

### Branch 1 uniquement (Data Lake)

```bash
spark-submit \
  --class Ex02DataIngestion \
  --master spark://spark-master:7077 \
  /opt/workdir/ex02_data_ingestion/target/scala-2.12/ex02-data-ingestion_2.12-0.1.0.jar \
  --year 2023 \
  --month 01
```

### Branch 1 + Branch 2 (Data Lake + PostgreSQL)

```bash
spark-submit \
  --class Ex02DataIngestion \
  --master spark://spark-master:7077 \
  --jars /opt/spark/jars/postgresql-42.7.4.jar \
  /opt/workdir/ex02_data_ingestion/target/scala-2.12/ex02-data-ingestion_2.12-0.1.0.jar \
  --year 2023 \
  --month 01 \
  --enableDw true
```

## Idempotence

- **Branch 1** : mode `overwrite` sur la partition mensuelle MinIO
- **Branch 2** : mode `overwrite` + `truncate` sur la table staging (mono-mois)

## Logs attendus

```
[INFO] Month window: [2023-01-01 00:00:00, 2023-02-01 00:00:00)
[INFO] Read raw: s3a://nyc-raw/yellow/2023/01/
[INFO] Raw count (before clean): 3,066,766
[INFO] Clean count (after month filter + generic checks): 2,950,000
[INFO] Write interim parquet: s3a://nyc-interim/yellow/2023/01/
[INFO] Branch 1 done.
[INFO] Write JDBC to staging table: public.yellow_trips_staging
[INFO] Branch 2 done.
[INFO] EX02 finished OK.
```

---

## Intégration Airflow (EX06)

Ce job est orchestré automatiquement par le DAG `full_nyc_taxi_pipeline` :

```python
ex02_spark_submit = BashOperator(
    task_id='ex02_spark_submit',
    bash_command="""
        docker exec spark-master spark-submit \
            --class Ex02DataIngestion \
            --master spark://spark-master:7077 \
            --jars /opt/spark/jars/postgresql-42.7.4.jar \
            /opt/workdir/ex02_data_ingestion/target/scala-2.12/ex02-data-ingestion_2.12-0.1.0.jar \
            --year {{ execution_date.year }} \
            --month {{ execution_date.strftime('%m') }} \
            --enableDw true
    """,
    sla=timedelta(hours=1, minutes=30),  # SLA: 1h30 max
)
```

**Caractéristiques :**
- 📅 Schedule : `@monthly`
- ⏱️ SLA : 1h30
- 🔄 Retries : 3 (avec 2 min de délai)
- ✅ Vérification post-exécution :
  - Branch 1 : présence des fichiers dans MinIO interim
  - Branch 2 : comptage des lignes dans staging PostgreSQL
- 📊 Quality check : vérification de la rétention des données (seuil 80%)

---

## Statut

✅ **Terminé et validé**

---

**Auteur :** MAAOUIA Ahmed – CY Tech Big Data
