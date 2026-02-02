# 🚀 Guide de Lancement - NYC Taxi Big Data Pipeline

## Prérequis

- Docker Desktop installé et lancé
- ~8 Go RAM disponible pour Docker
- Ports libres : 8080, 9000, 9001, 5432, 7077, 8081, 8501

---

## 1. Infrastructure principale

### Démarrer tous les services

```powershell
cd s:\PROJECTS\projects\nyc-taxi-bigdata-pipeline
docker-compose up -d
```

### Services démarrés

| Service | Port | URL/Accès |
|---------|------|-----------|
| Spark Master | 7077, 8081 | http://localhost:8081 |
| Spark Worker 1 | - | Via Spark Master |
| Spark Worker 2 | - | Via Spark Master |
| MinIO | 9000, 9001 | http://localhost:9001 |
| PostgreSQL | 5432 | `localhost:5432` |

### Credentials MinIO
- **Access Key** : `minioadmin`
- **Secret Key** : `minioadmin123`

### Credentials PostgreSQL
- **Host** : `localhost`
- **Port** : `5432`
- **Database** : `nyc_dw`
- **User** : `nyc`
- **Password** : `nyc123`

---

## 2. Airflow (Orchestration)

### Démarrer Airflow

```powershell
cd s:\PROJECTS\projects\nyc-taxi-bigdata-pipeline\ex06_airflow
docker-compose up -d
```

### Accès Interface Web
- **URL** : http://localhost:8080
- **Login** : `airflow`
- **Password** : `airflow`

### Commandes utiles

```powershell
# Voir les logs
docker-compose logs -f airflow-webserver

# Activer un DAG
docker exec -it airflow-webserver airflow dags unpause full_nyc_taxi_pipeline

# Trigger manuel
docker exec -it airflow-webserver airflow dags trigger full_nyc_taxi_pipeline

# Voir état des DAGs
docker exec -it airflow-webserver airflow dags list

# Voir les tasks d'un DAG
docker exec -it airflow-webserver airflow tasks list full_nyc_taxi_pipeline
```

---

## 3. Exécution manuelle des exercices (sans Airflow)

### EX01 - Data Retrieval (Scala/Spark)

```powershell
cd s:\PROJECTS\projects\nyc-taxi-bigdata-pipeline\ex01_data_retrieval

# Compiler
docker exec -it spark-master sbt compile

# Exécuter pour un mois spécifique
docker exec -it spark-master spark-submit \
  --class Ex01DataRetrieval \
  --master spark://spark-master:7077 \
  target/scala-2.12/ex01-data-retrieval_2.12-1.0.jar \
  --year 2023 --month 01
```

### EX02 - Data Ingestion (Scala/Spark)

```powershell
cd s:\PROJECTS\projects\nyc-taxi-bigdata-pipeline\ex02_data_ingestion

# Compiler
docker exec -it spark-master sbt compile

# Exécuter
docker exec -it spark-master spark-submit \
  --class Ex02DataIngestion \
  --master spark://spark-master:7077 \
  target/scala-2.12/ex02-data-ingestion_2.12-1.0.jar \
  --year 2023 --month 01
```

### EX03 - SQL/Data Warehouse

```powershell
# Connexion PostgreSQL
docker exec -it postgres psql -U postgres -d nyc_taxi_dw

# Ou exécuter les scripts SQL
docker exec -i postgres psql -U postgres -d nyc_taxi_dw < ex03_sql_table_creation/staging_creation.sql
docker exec -i postgres psql -U postgres -d nyc_taxi_dw < ex03_sql_table_creation/dw_creation.sql
docker exec -i postgres psql -U postgres -d nyc_taxi_dw < ex03_sql_table_creation/dw_load_reference.sql
docker exec -i postgres psql -U postgres -d nyc_taxi_dw < ex03_sql_table_creation/dw_load_incremental.sql
```

### EX04 - Dashboard Streamlit

```powershell
cd s:\PROJECTS\projects\nyc-taxi-bigdata-pipeline\ex04_dashboard\streamlit_app

# Option 1 : Via Docker
docker build -t nyc-dashboard -f ../../Docker/Dockerfile.streamlit .
docker run -p 8501:8501 --network nyc-net nyc-dashboard

# Option 2 : Local (Python)
pip install -r requirements.txt
streamlit run app.py
```
- **URL** : http://localhost:8501

### EX05 - ML Pipeline (Python/PySpark)

```powershell
cd s:\PROJECTS\projects\nyc-taxi-bigdata-pipeline\ex05_ml_prediction_service

# Option 1 : Script PowerShell complet
.\run_ml_pipeline.ps1

# Option 2 : Commandes individuelles
# Tests pré-entraînement
python -m pytest tests/test_validation.py -v

# Entraînement
python -m src.train --train-months 2023-01 2023-02 2023-03

# Prédiction
python -m src.predict --predict-month 2023-04

# Tests post-prédiction
python -m pytest tests/test_ml_quality.py -v
```

---

## 4. Vérifications et Monitoring

### Vérifier les buckets MinIO

```powershell
# Via interface web : z

# Ou via CLI mc (MinIO Client)
docker exec -it minio mc ls local/nyc-raw/
docker exec -it minio mc ls local/nyc-interim/
```

### Vérifier PostgreSQL

```powershell
# Nombre de lignes dans fact_trip
docker exec -it postgres psql -U postgres -d nyc_taxi_dw -c "SELECT COUNT(*) FROM fact_trip;"

# Vérifier les dimensions
docker exec -it postgres psql -U postgres -d nyc_taxi_dw -c "SELECT COUNT(*) FROM dim_location;"
docker exec -it postgres psql -U postgres -d nyc_taxi_dw -c "SELECT COUNT(*) FROM dim_vendor;"
```

### Vérifier Spark

- **Spark Master UI** : http://localhost:8081
- Voir les workers connectés et les jobs en cours

---

## 5. Arrêt des services

### Arrêt complet

```powershell
# Arrêter Airflow
cd s:\PROJECTS\projects\nyc-taxi-bigdata-pipeline\ex06_airflow
docker-compose down

# Arrêter l'infrastructure
cd s:\PROJECTS\projects\nyc-taxi-bigdata-pipeline
docker-compose down
```

### Arrêt avec suppression des volumes (reset complet)

```powershell
# ⚠️ Supprime toutes les données !
docker-compose down -v
```

---

## 6. Troubleshooting

### Problème : Port déjà utilisé
```powershell
# Trouver le processus
netstat -ano | findstr :8080

# Tuer le processus (remplacer PID)
taskkill /PID <PID> /F
```

### Problème : Pas assez de mémoire
```powershell
# Vérifier la mémoire Docker
docker system info | findstr Memory

# Nettoyer les images/containers inutilisés
docker system prune -a
```

### Problème : DAG non visible dans Airflow
```powershell
# Vérifier les erreurs de parsing
docker exec -it airflow-webserver airflow dags list-import-errors
```

### Problème : Spark job échoue
```powershell
# Voir les logs du worker
docker logs spark-worker-1
docker logs spark-worker-2
```

---

## 7. Ordre de lancement recommandé

```
1. docker-compose up -d              # Infrastructure (Spark, MinIO, PostgreSQL)
2. cd ex06_airflow && docker-compose up -d   # Airflow
3. http://localhost:8080             # Activer le DAG
4. http://localhost:8501             # Dashboard (optionnel)
```

---

## 8. Résumé des URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8080 | airflow / airflow |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin123 |
| Spark Master | http://localhost:8081 | - |
| Streamlit ML (EX05) | http://localhost:8501 | - |
| Streamlit Dashboard (EX04) | http://localhost:8502 | - |
| PostgreSQL | localhost:5432 | nyc / nyc123 |

---

## 9. Résultats d'Exécution (Janvier 2026)

### État du Pipeline Airflow

Le DAG `full_nyc_taxi_pipeline` a été exécuté avec succès pour la période **Janvier - Mai 2023** :

| Mois | execution_date | État | Durée |
|------|----------------|------|-------|
| Fév 2023 | 2023-01-31 | ✅ success | ~10 min |
| Mar 2023 | 2023-02-28 | ✅ success | ~14 min |
| Avr 2023 | 2023-03-31 | ✅ success | ~14 min |
| Mai 2023 | 2023-04-30 | ✅ success | ~3h (avec ML) |

### Data Warehouse - Données Chargées

```sql
SELECT DATE_TRUNC('month', pickup_date) as month, COUNT(*) as trips 
FROM fact_trip GROUP BY 1 ORDER BY 1;
```

| Mois | Nombre de trajets |
|------|-------------------|
| Décembre 2022 | 3,372,089 |
| Janvier 2023 | 6,083,028 |
| Février 2023 | 5,778,000 |
| Mars 2023 | 3,373,817 |
| Avril 2023 | 6,516,784 |
| **Total** | **~25 millions** |

### MinIO - Data Lake

**Buckets disponibles :**
- `nyc-raw/yellow/` : Données brutes (2022/12 + 2023/01-04)
- `nyc-interim/yellow/` : Données nettoyées (2022/12 + 2023/01-04)

### Modèle ML (EX05)

**Configuration :**
- **Algorithme** : GBTRegressor (Gradient Boosted Trees)
- **Training** : 9,065,096 lignes (Déc 2022 - Mar 2023)
- **Test** : 3,166,641 lignes (Avril 2023)

**Métriques obtenues :**
| Métrique | Valeur |
|----------|--------|
| RMSE | 5.84 |
| MAE | 2.16 |
| R² | **93.5%** |

**Durée d'entraînement** : ~1h44

Le modèle a été automatiquement promu dans le registry (`models/registry/current/`).