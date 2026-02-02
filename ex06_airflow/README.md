# Exercice 06 – Orchestration Airflow

## 🎯 Objectif

L'exercice **EX06** met en place **Apache Airflow** pour orchestrer automatiquement le pipeline Big Data complet (EX01 → EX05) de manière **mensuelle**, **idempotente** et **rattrapable** (backfill).

### ✨ Qualité et Monitoring (v2)

- **SLA** sur les tâches critiques (alertes si dépassement)
- **Vérification de comptage inter-étapes** (seuil 80% minimum)
- **Logging structuré** (timestamps, niveaux, traçabilité)
- **Tests unitaires DAGs** (validation structure et dépendances)

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           AIRFLOW                                       │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    full_nyc_taxi_pipeline                        │   │
│  │                     (DAG Principal)                              │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│         │                                                               │
│         ▼                                                               │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐              │
│  │   EX01      │     │   EX02      │     │   EX03      │              │
│  │ Retrieval   │────►│ Ingestion   │────►│  DW Load    │              │
│  │             │     │             │     │             │              │
│  └─────────────┘     └──────┬──────┘     └─────────────┘              │
│                             │                                          │
│                             │  ┌─────────────┐                        │
│                             └─►│   EX05      │                        │
│                                │  ML/MLOps   │                        │
│                                └─────────────┘                        │
└─────────────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌─────────────┐      ┌─────────────┐      ┌─────────────┐
│   MinIO     │      │ PostgreSQL  │      │   Spark     │
│  (S3A)      │      │   (DW)      │      │  Cluster    │
└─────────────┘      └─────────────┘      └─────────────┘
```

---

## 🗂️ Structure du Projet

```
ex06_airflow/
├── docker-compose.yml          # Configuration Airflow Docker
├── .env.example                # Variables d'environnement (template)
├── README.md                   # Ce fichier
├── pytest.ini                  # Configuration tests
├── dags/
│   ├── ex01_data_retrieval_dag.py    # DAG EX01 (standalone)
│   ├── ex02_data_ingestion_dag.py    # DAG EX02 (standalone)
│   ├── ex03_dw_dag.py                # DAG EX03 (standalone)
│   ├── ex05_ml_dag.py                # DAG EX05 (standalone)
│   └── full_pipeline_dag.py          # DAG COMPLET (recommandé, avec SLA)
├── tests/
│   ├── __init__.py
│   └── test_dags.py            # Tests unitaires DAGs
├── logs/                       # Logs Airflow
├── plugins/                    # Plugins custom (vide)
└── scripts/                    # Scripts auxiliaires
```

---

## 🚀 Démarrage Rapide

### 1. Prérequis

- Docker & Docker Compose installés
- Infrastructure principale démarrée (depuis la racine du projet) :
  ```bash
  docker-compose up -d
  ```

### 2. Configuration

```bash
cd ex06_airflow

# Copier et adapter les variables
cp .env.example .env

# Créer l'utilisateur Airflow (Linux/Mac)
echo "AIRFLOW_UID=$(id -u)" >> .env
```

### 3. Lancement Airflow

```bash
# Démarrer Airflow
docker-compose up -d

# Vérifier les logs d'initialisation
docker-compose logs -f airflow-init
```

### 4. Accès à l'Interface

- **URL** : http://localhost:8080
- **Login** : `airflow`
- **Password** : `airflow`

---

## 📋 Description des DAGs

### 🔹 `full_nyc_taxi_pipeline` (Recommandé)

**DAG principal qui orchestre tout le pipeline en une seule exécution.**

| Propriété | Valeur |
|-----------|--------|
| Schedule | `@monthly` |
| Start Date | 2023-01-01 |
| Catchup | ✅ Activé |
| Max Active Runs | 1 |

**Flux d'exécution :**
```
start → log_params → check_source
                          │
                          ▼
                    ┌─── EX01 ───┐
                    │            │
                    ▼            │
              ┌─── EX02 ───┐    │
              │            │    │
         Branch 1    Branch 2   │
              │            │    │
              │            ▼    │
              │         EX03    │
              │            │    │
              ▼            │    │
            EX05 ◄─────────┘    │
              │                 │
              ▼                 │
         pipeline_success ◄────┘
```

### 🔹 DAGs Individuels (Alternative)

Pour un contrôle plus fin, des DAGs individuels sont disponibles :

| DAG | Description | Dépendance |
|-----|-------------|------------|
| `ex01_data_retrieval` | Téléchargement + Upload MinIO | Aucune |
| `ex02_data_ingestion` | Nettoyage + Double branche | EX01 |
| `ex03_dw_loading` | Chargement Data Warehouse | EX02 |
| `ex05_ml_pipeline` | ML avec fenêtre glissante | EX02 |

---

## ⚙️ Configuration Technique

### Paramètres d'Orchestration

```python
# Période couverte (Janvier à Mai 2023)
start_date = datetime(2023, 1, 1)
end_date = datetime(2023, 5, 31)  # 5 mois de données

# Fréquence
schedule_interval = '@monthly'

# Backfill activé
catchup = True

# Un seul run à la fois
max_active_runs = 1
```

### Variables d'Environnement

| Variable | Description | Défaut |
|----------|-------------|--------|
| `MINIO_ENDPOINT` | Endpoint MinIO | `minio:9000` |
| `MINIO_ROOT_USER` | User MinIO | `minioadmin` |
| `MINIO_ROOT_PASSWORD` | Password MinIO | `minioadmin123` |
| `POSTGRES_HOST` | Host PostgreSQL | `postgres` |
| `POSTGRES_DB` | Base de données | `nyc_dw` |
| `POSTGRES_USER` | User PostgreSQL | `nyc` |
| `POSTGRES_PASSWORD` | Password PostgreSQL | `nyc123` |
| `SPARK_MASTER_URL` | URL Spark Master | `spark://spark-master:7077` |

---

## 🔄 Idempotence

Chaque exercice garantit l'idempotence :

| Exercice | Stratégie |
|----------|-----------|
| **EX01** | Skip si fichier existe + `overwrite` sur MinIO |
| **EX02** | `overwrite` sur partitions MinIO + `truncate` staging |
| **EX03** | `ON CONFLICT DO NOTHING` sur toutes les tables |
| **EX05** | Modèle "candidate" + promotion conditionnelle |

**Conséquence** : Le même mois peut être rejoué sans créer de doublons.

---

## 🔙 Backfill (Rattrapage)

### Via l'UI Airflow

1. Ouvrir le DAG `full_nyc_taxi_pipeline`
2. Cliquer sur le calendrier (icône)
3. Sélectionner les dates à rejouer
4. Trigger le DAG

### Via CLI

```bash
# Backfill janvier à mai 2023 (période configurée)
docker exec airflow-scheduler airflow dags backfill \
  --start-date 2023-01-01 \
  --end-date 2023-05-31 \
  full_nyc_taxi_pipeline
```

---

## 📊 Fenêtre Glissante ML (EX05)

Le pipeline ML utilise une stratégie de **fenêtre glissante** :

```
Mois traité : M (ex: Juin 2023)

┌───────────────────────────────────────────────────────┐
│   TRAINING (3 mois)              │   TEST (1 mois)   │
│   M-3     M-2     M-1            │        M          │
│  Mars    Avril    Mai            │      Juin         │
└───────────────────────────────────────────────────────┘
```

### Règle de Promotion du Modèle

Le nouveau modèle est promu si **au moins 2 métriques sur 3 s'améliorent** :

| Métrique | Direction |
|----------|-----------|
| RMSE | ↓ Plus bas = meilleur |
| MAE | ↓ Plus bas = meilleur |
| R² | ↑ Plus haut = meilleur |

---

## �️ Qualité et Monitoring

### SLA (Service Level Agreements)

Des SLA sont configurés sur les tâches critiques pour détecter les exécutions anormalement longues :

| Tâche | SLA | Description |
|-------|-----|-------------|
| `ex01_spark_submit` | 30 min | Download + upload MinIO |
| `ex02_spark_submit` | 1h30 | Nettoyage Spark + double branche |
| `ex03_load_fact_trip` | 1h | Chargement fact table |
| `ex05_run_ml_pipeline` | 2h30 | Training + évaluation ML |

**En cas de dépassement SLA :**
- Logs d'alerte dans Airflow
- Callback `sla_miss_callback` déclenché
- (Optionnel) Configuration d'alertes email

### Vérification Comptage Inter-Étapes

Une vérification automatique s'assure que les données ne sont pas perdues entre les étapes :

```
Seuils de rétention :
┌──────────────────────────────────────────┐
│  < 80%  │  ❌ FAIL  │ Perte critique     │
│  < 90%  │  ⚠️ WARN │ Alerte mais continue│
│  >= 90% │  ✅ OK    │ Rétention normale  │
└──────────────────────────────────────────┘
```

**Tâche concernée :** `ex02_quality_check_retention`

### Tests Unitaires DAGs

Des tests automatiques valident la structure des DAGs :

```bash
# Exécuter les tests
cd ex06_airflow
pytest tests/test_dags.py -v
```

**Tests inclus :**
- ✅ Chargement des DAGs sans erreur
- ✅ Présence de toutes les tâches critiques
- ✅ Absence de cycles
- ✅ Configuration SLA
- ✅ Dépendances correctes
- ✅ Schedule mensuel

---

## �🛠️ Dépannage

### Problème : Airflow ne démarre pas

```bash
# Vérifier les logs
docker-compose logs airflow-init
docker-compose logs airflow-scheduler

# Réinitialiser
docker-compose down -v
docker-compose up -d
```

### Problème : DAG non visible

```bash
# Forcer le parsing des DAGs
docker exec airflow-scheduler airflow dags list

# Vérifier les erreurs de syntaxe
docker exec airflow-scheduler python /opt/airflow/dags/full_pipeline_dag.py
```

### Problème : Task en échec

1. Ouvrir l'UI Airflow
2. Cliquer sur la tâche en échec
3. Consulter les logs
4. Corriger et "Clear" la tâche pour relancer

---

## 📝 Justification des Choix

### Pourquoi un DAG unique plutôt que plusieurs ?

| Critère | DAG Unique | Multi-DAGs |
|---------|------------|------------|
| **Lisibilité** | ✅ Vue globale | ⚠️ Dispersé |
| **Backfill** | ✅ Simple | ⚠️ Complexe |
| **Dépendances** | ✅ Explicites | ⚠️ ExternalTaskSensor |
| **Présentation** | ✅ Idéal jury | ⚠️ Plus complexe |

**Choix : DAG unique (`full_nyc_taxi_pipeline`)** pour la clarté et la simplicité d'utilisation.

### Pourquoi BashOperator plutôt que DockerOperator ?

- `spark-submit` doit s'exécuter depuis le conteneur Spark Master
- `docker exec` permet d'utiliser l'infrastructure existante
- Pas besoin de Docker-in-Docker (complexité réduite)

---

## 🎓 Comment Expliquer Airflow au Prof

### Définition Simple

> **Airflow** est un orchestrateur de workflows. Il permet de définir, planifier et surveiller des pipelines de données sous forme de **DAGs** (Directed Acyclic Graphs).

### Points Clés à Mentionner

1. **DAG** = Graphe de tâches avec dépendances
2. **Schedule** = Planification automatique (`@monthly`)
3. **Catchup** = Capacité à rejouer le passé (backfill)
4. **Idempotence** = Rejouer sans créer de doublons
5. **Monitoring** = Interface web pour suivre les exécutions

### Schéma pour le Jury

```
                   ┌────────────────┐
   Planificateur   │   SCHEDULER    │
                   └───────┬────────┘
                           │
                           ▼
                   ┌────────────────┐
   Définition      │     DAGs       │  ← Python
                   └───────┬────────┘
                           │
                           ▼
                   ┌────────────────┐
   Exécution       │    WORKERS     │  ← spark-submit, psql, etc.
                   └───────┬────────┘
                           │
                           ▼
                   ┌────────────────┐
   Monitoring      │   WEBSERVER    │  ← http://localhost:8080
                   └────────────────┘
```

### Vocabulaire à Maîtriser

| Terme | Définition |
|-------|------------|
| **DAG** | Directed Acyclic Graph - graphe orienté sans cycle |
| **Task** | Une étape du workflow (spark-submit, requête SQL...) |
| **Operator** | Type de tâche (BashOperator, PythonOperator...) |
| **Sensor** | Tâche qui attend une condition |
| **XCom** | Échange de données entre tâches |
| **Backfill** | Exécution rétroactive sur des dates passées |

---

## 📊 Composants Airflow

### Infrastructure Docker

| Composant        | Description                              | Port |
|------------------|------------------------------------------|------|
| Airflow Webserver| Interface web de monitoring              | 8080 |
| Airflow Scheduler| Planification et déclenchement des DAGs  | -    |
| Airflow Postgres | Base de métadonnées Airflow              | -    |

### Opérateurs Utilisés

| Opérateur | Usage |
|-----------|-------|
| `BashOperator` | Exécution de commandes shell (spark-submit, psql) |
| `PythonOperator` | Exécution de fonctions Python |
| `ShortCircuitOperator` | Skip conditionnel des tâches suivantes |
| `ExternalTaskSensor` | Attente de tâches d'autres DAGs |
| `EmptyOperator` | Points de synchronisation |

---

## ✅ Checklist Validation

- [x] Infrastructure principale démarrée (`docker-compose up -d` depuis la racine)
- [x] Airflow démarré (`docker-compose up -d` depuis ex06_airflow/)
- [x] DAG visible dans l'UI (http://localhost:8080)
- [x] DAG activé (toggle ON)
- [x] Trigger manuel réussi pour un mois
- [x] Backfill testé sur plusieurs mois (Fév-Mai 2023)
- [x] Logs consultables dans l'UI
- [x] EX05 ML exécuté avec succès (R² = 93.5%)

---

## 📚 Ressources

- [Documentation Airflow](https://airflow.apache.org/docs/)
- [Best Practices Airflow](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Tutoriel DAGs](https://airflow.apache.org/docs/apache-airflow/stable/tutorial.html)

---

## 📊 Statut

✅ **Terminé et validé**

### Résultats d'Exécution (Janvier 2026)

Le DAG `full_nyc_taxi_pipeline` a été exécuté avec succès :

| Mois | État | Tâches |
|------|------|--------|
| Fév 2023 | ✅ success | EX01 → EX02 → EX03 |
| Mar 2023 | ✅ success | EX01 → EX02 → EX03 |
| Avr 2023 | ✅ success | EX01 → EX02 → EX03 |
| Mai 2023 | ✅ success | EX01 → EX02 → EX03 → **EX05 (ML)** |

**Data Warehouse :** ~25 millions de trajets chargés (Déc 2022 - Avr 2023)

**Modèle ML :** R² = 93.5% (GBTRegressor promu automatiquement)

---

**Auteur :** MAAOUIA Ahmed – CY Tech Big Data

