# Exercice 01 – Data Retrieval

## Objectif

L'exercice **EX01** a pour objectif de mettre en place la **collecte initiale des données** NYC Yellow Taxi et leur intégration dans le Data Lake, sans transformation métier.

## Architecture

```
NYC TLC Website (Parquet)
         │
         ▼
   ┌─────────────┐
   │ Spark Job   │ (spark-submit)
   │ EX01        │
   └─────────────┘
         │
    ┌────┴────┐
    ▼         ▼
Local FS    MinIO
(Raw Zone)  (nyc-raw)
```

## Dataset utilisé

| Attribut        | Valeur                                      |
|-----------------|---------------------------------------------|
| Source          | NYC Taxi & Limousine Commission (TLC)       |
| Type            | Yellow Taxi Trip Records                    |
| Format          | Parquet                                     |
| Mois par défaut | Janvier 2023                                |

**URL source :**
```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
```

## Fonctionnalités

1. **Téléchargement** du fichier Parquet depuis l'URL officielle
2. **Stockage local** dans la Raw Zone (`data/raw/yellow/YYYY/MM/`)
3. **Lecture** du fichier via Spark
4. **Upload** vers MinIO (bucket `nyc-raw`)

## Idempotence

Le job est **idempotent** :
- Si le fichier existe déjà localement, le téléchargement est ignoré
- L'écriture vers MinIO utilise le mode `overwrite` sur la partition mensuelle

## Technologies

| Composant       | Version / Type    |
|-----------------|-------------------|
| Langage         | Scala 2.12        |
| Framework       | Apache Spark 3.5  |
| Build tool      | SBT               |
| Storage         | MinIO (S3A)       |

## Structure du projet

```
ex01_data_retrieval/
├── build.sbt
├── project/
│   └── build.properties
└── src/
    └── main/
        └── scala/
            └── Ex01DataRetrieval.scala
```

## Paramètres CLI

| Paramètre   | Description             | Défaut |
|-------------|-------------------------|--------|
| `--year`    | Année à traiter         | 2023   |
| `--month`   | Mois à traiter (01-12)  | 01     |

## Emplacements des données

| Zone            | Chemin                                          |
|-----------------|------------------------------------------------|
| Local (Raw)     | `/opt/data/raw/yellow/YYYY/MM/`                |
| MinIO (S3A)     | `s3a://nyc-raw/yellow/YYYY/MM/`                |

## Compilation

```bash
cd ex01_data_retrieval
sbt package
```

Le JAR généré se trouve dans `target/scala-2.12/`.

## Exécution

Depuis le conteneur Spark Master :

```bash
spark-submit \
  --class Ex01DataRetrieval \
  --master spark://spark-master:7077 \
  /opt/workdir/ex01_data_retrieval/target/scala-2.12/ex01-data-retrieval_2.12-0.1.0.jar \
  --year 2023 \
  --month 01
```

## Dépendances

Le job utilise les dépendances Spark fournies par le cluster (`provided`) :

```scala
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1" % "provided",
  "org.apache.spark" %% "spark-sql"  % "3.5.1" % "provided"
)
```

## Logs attendus

```
[INFO] Downloading NYC Taxi file from https://...
[INFO] File downloaded to /opt/data/raw/yellow/2023/01/yellow_tripdata_2023-01.parquet
[INFO] Number of records: 3,066,766
[INFO] File uploaded to s3a://nyc-raw/yellow/2023/01/
```

---

## Intégration Airflow (EX06)

Ce job est orchestré automatiquement par le DAG `full_nyc_taxi_pipeline` :

```python
ex01_spark_submit = BashOperator(
    task_id='ex01_spark_submit',
    bash_command="""
        docker exec spark-master spark-submit \
            --class Ex01DataRetrieval \
            --master spark://spark-master:7077 \
            /opt/workdir/ex01_data_retrieval/target/scala-2.12/ex01-data-retrieval_2.12-0.1.0.jar \
            --year {{ execution_date.year }} \
            --month {{ execution_date.strftime('%m') }}
    """,
    sla=timedelta(minutes=30),  # SLA: 30 minutes max
)
```

**Caractéristiques :**
- 📅 Schedule : `@monthly`
- ⏱️ SLA : 30 minutes
- 🔄 Retries : 3 (avec 2 min de délai)
- ✅ Vérification post-exécution : présence des fichiers dans MinIO

---

## Statut

✅ **Terminé et validé**

---

**Auteur :** MAAOUIA Ahmed – CY Tech Big Data
