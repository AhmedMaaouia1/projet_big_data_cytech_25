# Exercice 05 – Machine Learning & MLOps (Spark MLlib)

## Objectif

L'exercice **EX05** s'inscrit dans la continuité du pipeline Big Data mis en place dans les exercices précédents (EX01 à EX04). Son objectif est d'introduire une première démarche **MLOps**, en implémentant un modèle de machine learning distribué à l'aide de **Spark MLlib**, depuis la lecture des données jusqu'à l'inférence.

### Objectifs principaux

- ✅ Entraîner un modèle ML à partir de données stockées dans MinIO
- ✅ Prédire le montant total payé (`total_amount`) pour une course de taxi
- ✅ Évaluer la qualité du modèle avec des métriques standards
- ✅ Garantir la qualité logicielle (PEP8, tests unitaires, documentation)
- ✅ Préparer une architecture compatible avec des évolutions MLOps futures
- ✅ **Fenêtre glissante** pour entraînement mensuel automatisé
- ✅ **Model Registry** avec promotion automatique des modèles

---

## 🆕 Stratégie ML v2 - Fenêtre Glissante

### Principe

Le pipeline ML utilise une **stratégie de fenêtre glissante** compatible avec une orchestration Airflow mensuelle :

```
┌─────────────────────────────────────────────────────────────┐
│                    FENÊTRE GLISSANTE                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   Training Window (3 mois)          Test (1 mois)           │
│   ┌───────┬───────┬───────┐         ┌───────┐               │
│   │ M-3   │ M-2   │ M-1   │    →    │   M   │               │
│   │ Mars  │ Avril │ Mai   │         │ Juin  │               │
│   └───────┴───────┴───────┘         └───────┘               │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Règle de Promotion

Le nouveau modèle (candidate) est promu si **au moins 2 métriques sur 3 s'améliorent** :

| Métrique | Direction d'amélioration |
|----------|--------------------------|
| RMSE     | ↓ Plus bas = meilleur    |
| MAE      | ↓ Plus bas = meilleur    |
| R²       | ↑ Plus haut = meilleur   |

### Model Registry

Structure simple avec 2 slots maximum :

```
models/registry/
├── model_registry.json     # Métadonnées et historique
├── current/                # Modèle en production
│   ├── metadata/
│   └── stages/
└── candidate/              # Nouveau modèle à évaluer
    ├── metadata/
    └── stages/
```

### Exécution Mensuelle (Airflow)

```bash
# Auto-détection du mois courant
python src/ml_pipeline.py

# Appel avec mois de test explicite
python src/ml_pipeline.py --test-month 2023-06

# Appel avec mois d'entraînement explicites
python src/ml_pipeline.py \
    --train-months 2023-03,2023-04,2023-05 \
    --test-month 2023-06 \
    --model-registry-path models/registry
```

### Arguments CLI

| Argument               | Description                                      | Requis |
|------------------------|--------------------------------------------------|--------|
| `--test-month`         | Mois de test (ex: 2023-06). Si omis, utilise le mois courant | ❌ |
| `--train-months`       | Mois d'entraînement (ex: 2023-03,2023-04,2023-05)| ❌     |
| `--model-registry-path`| Chemin du registry (défaut: models/registry)     | ❌     |
| `--data-base-path`     | Chemin données MinIO (défaut: s3a://nyc-interim/yellow) | ❌ |
| `--reports-dir`        | Répertoire rapports (défaut: reports)            | ❌     |
| `--dry-run`            | Validation des données sans exécution du training | ❌    |
| `--skip-missing`       | Continue avec les données disponibles si certains mois manquent | ❌ |

### Gestion des données manquantes

Le pipeline valide automatiquement la disponibilité des données avant l'entraînement.

#### Règles de validation

| Condition | Résultat |
|-----------|----------|
| Mois de **test** manquant | ❌ **ERREUR** (toujours obligatoire) |
| < 2 mois de **training** disponibles | ❌ **ERREUR** (minimum requis) |
| ≥ 2 mois de **training** disponibles | ✅ Continue |

#### Mode strict (par défaut)

```bash
python src/ml_pipeline.py --test-month 2023-06
```
→ Si **n'importe quel mois** manque → **ERREUR** avec message explicite

#### Mode tolérant (`--skip-missing`)

```bash
python src/ml_pipeline.py --test-month 2023-06 --skip-missing
```
→ Continue avec les données disponibles SI :
- ✅ Le mois de test existe
- ✅ Au moins 2 mois de training existent

#### Exemple de message d'erreur

```
❌ DATA NOT FOUND
==================================================
Missing months: ['2023/03', '2023/04']
Available months: ['2023/01', '2023/02', '2023/05']

Options:
  1. Ingest the missing data first
  2. Use --skip-missing to continue with available data
     (requires at least 2 training months + test month)
  3. Use --test-month to specify a different month
```

---

## Architecture

```
MinIO (nyc-interim)
        │
        ▼
┌─────────────────────────┐
│    Spark DataFrame      │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   Feature Engineering   │
│   (features.py)         │
└───────────┬─────────────┘
            │
            ▼
┌─────────────────────────┐
│   Pipeline ML Spark     │
│   (StringIndexer +      │
│    OneHotEncoder +      │
│    VectorAssembler +    │
│    GBTRegressor)        │
└───────────┬─────────────┘
            │
       ┌────┴────┐
       │         │
       ▼         ▼
   Training   Inference
       │         │
       ▼         ▼
   Metrics    Predictions
   (reports/) (reports/)
```

### Composants

| Composant       | Description                          |
|-----------------|--------------------------------------|
| Spark Cluster   | 1 Master + 2 Workers (Docker)        |
| Data Lake       | MinIO (S3 compatible)                |
| ML Framework    | Spark MLlib                          |
| Langage         | Python (scripts uniquement)          |

---

## Structure du projet

```
ex05_ml_prediction_service/
├── main.py                      # Point d'entrée principal
├── pyproject.toml               # Configuration projet Python
├── run_pipeline.ps1             # Orchestrateur principal (legacy + sliding window)
├── run_tests_pre_train.ps1      # Tests avant entraînement
├── run_ex05.ps1                 # Entraînement seul (legacy)
├── run_tests_post_train.ps1     # Tests qualité après entraînement
├── run_predict.ps1              # Inférence seule
├── run_tests_post_predict.ps1   # Tests plausibilité métier
├── conf/
│   └── log4j2.properties        # Configuration logging Spark
├── models/
│   ├── ex05_spark_model/        # Modèle entraîné (legacy)
│   └── registry/                # Model Registry (sliding window)
│       ├── model_registry.json  # Métadonnées & historique
│       ├── current/             # Modèle en production
│       └── candidate/           # Nouveau modèle candidat
├── reports/
│   ├── eda_sample.csv           # Échantillon EDA
│   ├── eda_summary.json         # Résumé statistique
│   ├── error_summary.json       # Analyse d'erreurs globale
│   ├── error_by_price_bucket.json # Erreurs par tranche de prix
│   ├── predict_report.json      # Rapport d'inférence
│   └── train_metrics.json       # Métriques d'entraînement
├── src/
│   ├── __init__.py
│   ├── config.py           # Configuration centralisée
│   ├── eda.py              # Exploration des données
│   ├── error_analysis.py   # Analyse d'erreurs post-prédiction
│   ├── features.py         # Feature engineering
│   ├── logging_config.py   # Configuration logging centralisée ✨
│   ├── ml_pipeline.py      # Pipeline ML orchestrable (sliding window)
│   ├── model_registry.py   # Gestion du model registry
│   ├── predict.py          # Module d'inférence
│   ├── spark_io.py         # I/O Spark (MinIO)
│   ├── spark_prepare.py    # Préparation Spark
│   ├── spark_session.py    # Création session Spark
│   ├── train.py            # Module d'entraînement (legacy)
│   ├── trainer.py          # Trainer modulaire
│   ├── utils.py            # Utilitaires
│   └── validation.py       # Validation des données
└── tests/
    ├── test_ml_plausibility.py  # Tests plausibilité métier
    ├── test_ml_quality.py       # Tests qualité modèle
    ├── test_ml_schema.py        # Tests schéma ML
    ├── test_model_registry.py   # Tests model registry
    ├── test_month_range.py      # Tests plage de mois
    └── test_validation.py       # Tests validation
```

---

## Données utilisées

### Source

Les données proviennent du dataset officiel **NYC Yellow Taxi Trips**, stockées sous forme de fichiers Parquet dans MinIO (`nyc-interim`).

### Périmètre temporel

#### Mode Legacy (train.py)
| Phase         | Mois utilisés       |
|---------------|---------------------|
| Entraînement  | 2023/01, 2023/02    |
| Inférence     | 2023/02             |

#### Mode Fenêtre Glissante (ml_pipeline.py)
| Phase         | Description                              |
|---------------|------------------------------------------|
| Entraînement  | M-3, M-2, M-1 (3 derniers mois)          |
| Test          | M (mois courant)                         |

### Target

- **`total_amount`** : montant total payé pour une course

---

## Feature Engineering

Les features sont construites dans `src/features.py` et sont **strictement identiques** entre entraînement et inférence.

### Variables numériques

| Feature           | Description                    |
|-------------------|--------------------------------|
| trip_distance     | Distance du trajet (miles)     |
| passenger_count   | Nombre de passagers            |
| trip_duration_min | Durée du trajet (minutes)      |
| pickup_hour       | Heure de prise en charge       |
| pickup_dayofweek  | Jour de la semaine (0-6)       |
| pickup_month      | Mois (1-12)                    |

### Variables catégorielles

Encodées via `StringIndexer` + `OneHotEncoder` :

| Feature           | Description                    |
|-------------------|--------------------------------|
| VendorID          | Fournisseur                    |
| RatecodeID        | Code tarifaire                 |
| payment_type      | Type de paiement               |
| store_and_fwd_flag| Flag store and forward         |
| PULocationID      | Zone de départ                 |
| DOLocationID      | Zone d'arrivée                 |

Toutes les features sont assemblées via `VectorAssembler`.

---

## Modèle

### Algorithme sélectionné

**Gradient Boosted Trees Regressor (GBTRegressor)**

### Justification

- ✅ Performant sur des relations non linéaires
- ✅ Robuste face aux interactions complexes entre variables
- ✅ Très adapté aux données tabulaires hétérogènes
- ✅ Disponible nativement dans Spark MLlib
- ✅ Interprétable via feature importance

### Hyperparamètres

| Paramètre  | Valeur |
|------------|--------|
| maxDepth   | 6      |
| maxIter    | 50     |
| seed       | 42     |

---

## Entraînement

### Split des données

| Ensemble      | Proportion |
|---------------|------------|
| Train         | 80%        |
| Test          | 20%        |

### Pipeline Spark ML

Le pipeline inclut les étapes suivantes :

1. `StringIndexer` (variables catégorielles)
2. `OneHotEncoder`
3. `VectorAssembler`
4. `GBTRegressor`

Le modèle final est sauvegardé dans :
```
models/ex05_spark_model/
```

---

## Évaluation

### Métriques utilisées

| Métrique | Description                          |
|----------|--------------------------------------|
| RMSE     | Root Mean Square Error               |
| MAE      | Mean Absolute Error                  |
| R²       | Coefficient de détermination         |

### Résultats obtenus

```json
{
  "rmse": 5.17,
  "mae": 2.05,
  "r2": 0.94
}
```
---

## Application Streamlit – Visualisation et Démonstration

Une application **Streamlit** a été développée pour permettre l'exploration interactive des résultats du modèle de prédiction et l'analyse des données. Cette interface facilite la visualisation des prédictions, des métriques et des analyses d'erreurs, tout en offrant une expérience utilisateur simple et efficace.

### Fonctionnalités principales

- Visualisation des prédictions du modèle sur des échantillons de données
- Affichage des métriques de performance (RMSE, MAE, R²)
- Analyse d'erreurs par tranche de prix
- Exploration interactive des données d'entrée et des résultats

---
### Aperçu de l'application

Ci-dessous quelques captures d'écran de l'application Streamlit :

<p align="center">
  <img src="../Documents/Captures/ML%20streamlit/1.png" alt="Accueil Streamlit" width="600"/>
  <br/>
  <img src="../Documents/Captures/ML%20streamlit/2.png" alt="Métriques et prédictions" width="600"/>
  <br/>
  <img src="../Documents/Captures/ML%20streamlit/3.png" alt="Analyse d'erreurs" width="600"/>
  <br/>
  <img src="../Documents/Captures/ML%20streamlit/4.png" alt="Exploration interactive" width="600"/>
</p>

L'application est accessible dans le dossier : `ex05_ml_prediction_service/streamlit_app/app.py`.

Pour lancer l'application :

```bash
cd ex05_ml_prediction_service/streamlit_app
streamlit run app.py
```
---

> 👉 La contrainte de l'énoncé (**RMSE < 10**) est largement respectée.

Les métriques sont stockées dans : `reports/train_metrics.json`

---

## Analyse d'erreurs (Error Analysis)

Une analyse d'erreurs complète est effectuée après l'évaluation du modèle, permettant de comprendre **où et pourquoi** le modèle se trompe.

### Métriques calculées

| Métrique              | Description                                  |
|-----------------------|----------------------------------------------|
| mean_error            | Erreur moyenne (biais du modèle)             |
| std_error             | Écart-type des erreurs                       |
| median_error          | Erreur médiane                               |
| p95_error / p99_error | Percentiles 95 et 99 des erreurs             |
| pct_underestimate     | % de cas où le modèle sous-estime            |
| pct_overestimate      | % de cas où le modèle surestime              |

### Analyse par tranche de prix

Les erreurs sont analysées par bucket de prix :

| Bucket     | Plage ($)   | Description                    |
|------------|-------------|--------------------------------|
| low        | 0 - 10      | Courses courtes / minimum fare |
| medium     | 10 - 30     | Courses standard               |
| high       | 30 - 60     | Courses moyennes-longues       |
| very_high  | > 60        | Airport, longue distance       |

### Identification des causes d'erreurs

Pour les 10 plus fortes erreurs, le système identifie automatiquement les causes probables :

- `long_distance_low_fare_anomaly` : anomalie de données
- `short_trip_high_fare_tips_or_surge` : pourboire ou surge pricing
- `extended_duration_traffic_or_wait` : trafic ou attente
- `night_surge_pricing` : tarification de nuit
- `airport_flat_rate` : tarif forfaitaire aéroport
- `cash_payment_tip_not_recorded` : pourboire cash non enregistré
- `negotiated_fare` : tarif négocié

### Artefacts générés

| Fichier                      | Contenu                              |
|------------------------------|--------------------------------------|
| `error_summary.json`         | Statistiques globales + insights     |
| `error_by_price_bucket.json` | Erreurs par tranche de prix          |

---

## Inférence

L'inférence est réalisée à partir du modèle sauvegardé, sur un mois distinct.

### Étapes

1. Chargement du modèle Spark ML
2. Lecture des données depuis MinIO
3. Application du même feature engineering
4. Validation des données d'entrée (échantillon)
5. Génération des prédictions

---

## Validation des données

Un module dédié (`src/validation.py`) vérifie :

- ✅ Présence des colonnes obligatoires
- ✅ Absence de valeurs négatives incohérentes
- ✅ Cohérence temporelle (pickup ≤ dropoff)

La validation est appliquée :
- Avant entraînement
- Avant inférence (sur un échantillon)

---

## Qualité logicielle

### Standards respectés

| Standard        | Outil          | Statut |
|-----------------|----------------|--------|
| PEP8            | flake8         | ✅     |
| Auto-formatting | autopep8       | ✅     |
| Documentation   | NumpyDoc       | ✅     |
| Tests           | pytest         | ✅     |

### Vérification PEP8

```bash
flake8 src/
```

---

## Tests Machine Learning

Des tests spécifiques au ML sont implémentés pour garantir la robustesse du pipeline.

### Organisation des tests

| Fichier                    | Phase           | Script associé               |
|----------------------------|-----------------|------------------------------|
| `test_validation.py`       | Pre-training    | `run_tests_pre_train.ps1`    |
| `test_month_range.py`      | Pre-training    | `run_tests_pre_train.ps1`    |
| `test_ml_schema.py`        | Pre-training    | `run_tests_pre_train.ps1`    |
| `test_ml_quality.py`       | Post-training   | `run_tests_post_train.ps1`   |
| `test_ml_plausibility.py`  | Post-prediction | `run_tests_post_predict.ps1` |

### Tests Pre-training (`run_tests_pre_train.ps1`)

Vérifient que les données sont prêtes pour l'entraînement :

- ✅ Colonnes obligatoires présentes
- ✅ Valeurs non négatives
- ✅ Cohérence temporelle (pickup ≤ dropoff)
- ✅ Ranges de mois valides

### Tests Post-training (`run_tests_post_train.ps1`)

Vérifient les seuils de performance du modèle :

| Test                | Seuil                    | Description                  |
|---------------------|--------------------------|------------------------------|
| RMSE                | < 10.0                   | Exigence du projet           |
| R²                  | > 0.0                    | Meilleur que la moyenne      |
| R² (acceptable)     | > 0.5                    | Bon modèle                   |
| MAE                 | < 15.0                   | Erreur absolue raisonnable   |
| MAE ≤ RMSE          | Toujours                 | Cohérence mathématique       |

### Tests Post-prediction (`run_tests_post_predict.ps1`)

Vérifient que les prédictions sont métier-valides :

- ✅ Prédictions non négatives (tarifs ≥ 0)
- ✅ Prédictions finies (pas de NaN/Inf)
- ✅ Prédictions raisonnables (< $500 pour un trajet)
- ✅ Contraintes métier NYC Taxi respectées

---

## Exécution

### Workflow complet MLOps

Le projet suit un workflow MLOps structuré avec des scripts dédiés à chaque étape :

```
┌─────────────────────────────────────────────────────────────────┐
│                     WORKFLOW MLOps EX05                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. PRE-TRAINING TESTS                                          │
│     .\run_tests_pre_train.ps1                                   │
│     "Est-ce que j'ai le droit d'entraîner ?"                    │
│                          │                                      │
│                          ▼                                      │
│  2. TRAINING + ERROR ANALYSIS                                   │
│     .\run_ex05.ps1                                              │
│     "Je produis un modèle et ses artefacts"                     │
│                          │                                      │
│                          ▼                                      │
│  3. POST-TRAINING QUALITY TESTS                                 │
│     .\run_tests_post_train.ps1                                  │
│     "Ce modèle a-t-il le droit d'exister ?"                     │
│                          │                                      │
│                          ▼                                      │
│  4. PREDICTION / INFERENCE                                      │
│     .\run_predict.ps1                                           │
│     "Je fais de la prédiction"                                  │
│                          │                                      │
│                          ▼                                      │
│  5. POST-PREDICTION PLAUSIBILITY TESTS                          │
│     .\run_tests_post_predict.ps1                                │
│     "Les résultats sont acceptables pour un humain ?"           │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Scripts disponibles

| Script                        | Rôle                                      | Question clé                              |
|-------------------------------|-------------------------------------------|-------------------------------------------|
| `run_pipeline.ps1`            | **Orchestrateur complet** (legacy + sliding window) | "Pipeline ML complet"             |
| `run_tests_pre_train.ps1`     | Tests schéma, validation, ranges          | "Puis-je entraîner ?"                     |
| `run_ex05.ps1`                | Entraînement Spark (legacy)               | "Produire modèle + artefacts"             |
| `run_tests_post_train.ps1`    | Tests RMSE < 10, R² > 0, MAE              | "Ce modèle est-il valide ?"               |
| `run_predict.ps1`             | Inférence Spark                           | "Prédiction contrôlée"                    |
| `run_tests_post_predict.ps1`  | Tests plausibilité métier                 | "Résultats acceptables ?"                 |

### Orchestrateur principal (`run_pipeline.ps1`)

Le script `run_pipeline.ps1` est l'**orchestrateur principal** qui supporte les deux modes :

#### Paramètres

| Paramètre       | Description                                              |
|-----------------|----------------------------------------------------------|
| `-TestMonth`    | Mois de test (ex: 2023-06). Si omis, utilise le mois courant |
| `-TrainMonths`  | Mois d'entraînement explicites (ex: 2023-03,2023-04,2023-05) |
| `-SkipMissing`  | Continue avec les données disponibles si certains mois manquent |
| `-Legacy`       | Utilise l'ancien `train.py` avec mois hardcodés          |
| `-SkipTests`    | Saute les tests de validation (non recommandé)           |
| `-TrainOnly`    | Exécute uniquement la phase d'entraînement               |
| `-PredictOnly`  | Exécute uniquement la phase d'inférence                  |
| `-Force`        | Force le ré-entraînement même si un modèle existe        |
| `-RegistryPath` | Chemin du registry (défaut: models/registry)             |
| `-Help`         | Affiche l'aide                                           |

#### Exemples d'utilisation

```powershell
cd ex05_ml_prediction_service

# Pipeline complet avec auto-détection du mois
.\run_pipeline.ps1

# Mois de test explicite
.\run_pipeline.ps1 -TestMonth "2023-05"

# Continuer même si certains mois manquent (min 2 mois de training requis)
.\run_pipeline.ps1 -TestMonth "2023-05" -SkipMissing

# Mode legacy (ancien train.py avec 2023/01-02)
.\run_pipeline.ps1 -Legacy

# Entraînement seul
.\run_pipeline.ps1 -TestMonth "2023-05" -TrainOnly

# Forcer le ré-entraînement
.\run_pipeline.ps1 -TestMonth "2023-05" -Force
```

### Exécution pas à pas (mode legacy)

```powershell
cd ex05_ml_prediction_service

# 1. Valider les données avant entraînement
.\run_tests_pre_train.ps1

# 2. Entraîner le modèle
.\run_ex05.ps1

# 3. Vérifier la qualité du modèle
.\run_tests_post_train.ps1

# 4. Lancer l'inférence
.\run_predict.ps1

# 5. Valider les prédictions
.\run_tests_post_predict.ps1
```

### Commande spark-submit (manuel)

```bash
spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.driver.extraJavaOptions=-Dlog4j.configurationFile=conf/log4j2.properties \
  src/train.py
```

---

## Variables d'environnement

| Variable             | Description                      | Défaut           |
|----------------------|----------------------------------|------------------|
| MINIO_ENDPOINT       | URL MinIO                        | http://minio:9000|
| MINIO_ACCESS_KEY     | Access key MinIO                 | -                |
| MINIO_SECRET_KEY     | Secret key MinIO                 | -                |
| MINIO_BUCKET_INTERIM | Bucket données nettoyées         | nyc-interim      |

---

## Performances

| Phase         | Durée approximative |
|---------------|---------------------|
| Entraînement  | ~4032 s             |
| Inférence     | ~84 s               |

---

## Limitations connues

- ❌ Pas de tuning automatique des hyperparamètres
- ❌ Pas de gestion de dérive (data drift)
- ❌ Pas de monitoring temps réel
- ❌ Inférence batch uniquement

---

## Améliorations implémentées

- ✅ **Analyse d'erreurs complète** : statistiques, buckets, causes métier
- ✅ **Tests ML orientés** : schéma, plausibilité, qualité
- ✅ **Insights automatiques** : justification métier des erreurs élevées
- ✅ **Fenêtre glissante** : entraînement sur 3 mois, test sur le mois suivant
- ✅ **Model Registry** : gestion current/candidate avec promotion automatique
- ✅ **Règle de promotion** : 2/3 métriques doivent s'améliorer (RMSE, MAE, R²)
- ✅ **Validation des données** : vérification de disponibilité avant entraînement
- ✅ **Mode tolérant** : continue avec données disponibles (min 2 mois training)
- ✅ **Auto-détection du mois** : utilise le mois courant si non spécifié
- ✅ **Idempotence** : script exécutable plusieurs fois sans effet de bord

---

## Perspectives futures

Plusieurs extensions MLOps sont envisageables :

- 🔄 Intégration Airflow pour orchestration mensuelle automatisée (EX06)
- 📦 Versionnement avancé du modèle (MLflow)
- 🚀 Déploiement via API ou front-end
- 📈 Monitoring et détection de dérive
- 🔧 Hyperparameter tuning automatique

---

## 📋 Logging Structuré

### Configuration Centralisée

Le module `logging_config.py` fournit un système de logging standardisé pour tout le pipeline ML.

### Format des Logs

```
2024-06-15 14:30:25 | INFO     | src.ml_pipeline            | Pipeline started
2024-06-15 14:30:26 | INFO     | src.trainer                | Training on 2,500,000 rows
2024-06-15 14:35:42 | WARNING  | src.ml_pipeline            | Missing data for ['2023/03']
```

### Utilisation

```python
from logging_config import get_logger, PipelineLogger

# Logger simple
logger = get_logger(__name__)
logger.info("Message")
logger.warning("Attention")
logger.error("Erreur")

# Logger avec tracking de métriques
pipeline_log = PipelineLogger("MLPipeline")
pipeline_log.stage_start("training", months=["2023/01", "2023/02"])
# ... training ...
pipeline_log.stage_end("training", row_count=2500000)
pipeline_log.verify_retention("load_raw", "after_cleaning", min_threshold=0.80)
pipeline_log.summary()
```

### Classes Disponibles

| Classe | Description |
|--------|-------------|
| `get_logger(name)` | Logger standard avec format uniforme |
| `PipelineLogger` | Logger avec tracking de métriques et vérification de rétention |
| `configure_file_logging()` | Ajoute l'écriture vers fichier |

### Avantages

- ✅ **Timestamps** : Traçabilité temporelle complète
- ✅ **Niveaux** : INFO, WARNING, ERROR pour filtrage
- ✅ **Modules** : Identification de la source du log
- ✅ **Rétention** : Vérification automatique perte de données
- ✅ **Production-ready** : Compatible avec ELK, CloudWatch, etc.

---

## Intégration Airflow (EX06)

Le script `ml_pipeline.py` est conçu pour être appelé par Airflow :

```python
from airflow.operators.bash import BashOperator

train_task = BashOperator(
    task_id='train_model',
    bash_command='cd /opt/workdir/ex05_ml_prediction_service && python src/ml_pipeline.py --test-month {{ ds[:7] }}'
)
```

Le script est **idempotent** : toute la logique métier (comparaison, promotion) reste dans EX05.

---

## Conclusion

Cet exercice constitue une première implémentation complète d'un **pipeline ML distribué**, intégrée dans une architecture Big Data existante. Il pose les bases solides d'une démarche MLOps, tout en respectant les contraintes industrielles : **reproductibilité**, **qualité du code**, **traçabilité** et **performance**.

---

---

## Statut

✅ **Terminé et validé**

---

**Auteur :** MAAOUIA Ahmed – CY Tech Big Data
