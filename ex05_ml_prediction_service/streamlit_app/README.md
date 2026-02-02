# 🚕 NYC Taxi ML - Application Streamlit

Application de démonstration et validation du modèle de Machine Learning pour la prédiction des tarifs de taxis à New York City.

## 🎯 Objectif

Cette application Streamlit offre une interface professionnelle pour :

- **Démontrer** le fonctionnement du modèle ML
- **Valider** la qualité des prédictions
- **Analyser** les erreurs et biais du modèle
- **Documenter** les limites et cas d'usage

## 👥 Public Cible

Cette application est destinée aux **utilisateurs internes** :

- 📊 Data Scientists & Data Engineers
- 💼 Équipes métier (analyse tarifaire)
- 🎯 Équipes produit (évaluation de faisabilité)
- 🎓 Formations aux techniques ML/Spark

> ⚠️ **Note**: Cette application n'est PAS destinée au grand public.

## 🖥️ Fonctionnalités

### Section 1 - Présentation
- Description du modèle et de l'architecture
- Informations sur les données NYC TLC
- Status de disponibilité du modèle

### Section 2 - Prédiction Manuelle
- Formulaire interactif avec tous les paramètres métier
- Sélection des zones NYC avec noms descriptifs
- Prédiction en temps réel via le modèle Spark ML
- Affichage du tarif estimé avec contexte

### Section 3 - Qualité du Modèle
- Métriques de performance (RMSE, MAE, R²)
- Visualisations interactives (gauges, charts)
- Informations sur les données d'entraînement
- Hyperparamètres utilisés

### Section 4 - Analyse des Erreurs
- Distribution et biais des erreurs
- Analyse par tranche de prix
- Percentiles d'erreur (P25, P50, P75, P95, P99)
- Insights métier automatiques

### Section 5 - Limites & Cadre d'Utilisation
- Limitations techniques et métier
- Cas d'usage appropriés
- Recommandations d'utilisation

## 🐳 Lancement via Docker

### Prérequis

- Docker et Docker Compose installés
- Le projet NYC Taxi Big Data Pipeline configuré
- Variables d'environnement définies dans `.env`

### Démarrage

```bash
# Depuis la racine du projet
docker compose up -d streamlit

# Ou pour tout démarrer (Spark, MinIO, Streamlit)
docker compose up -d
```

### Accès

Une fois démarré, l'application est accessible à :

```
http://localhost:8501
```

### Arrêt

```bash
docker compose stop streamlit

# Ou pour tout arrêter
docker compose down
```

## 📁 Structure des Fichiers

```
ex05_ml_prediction_service/
├── streamlit_app/
│   ├── app.py              # Application Streamlit principale
│   ├── requirements.txt    # Dépendances Python
│   └── README.md           # Cette documentation
├── models/
│   └── ex05_spark_model/   # Modèle Spark ML pré-entraîné
└── reports/
    ├── train_metrics.json      # Métriques d'entraînement
    ├── error_summary.json      # Résumé des erreurs
    └── error_by_price_bucket.json  # Erreurs par tranche
```

## ⚙️ Configuration

L'application lit automatiquement les artefacts suivants :

| Fichier | Description | Obligatoire |
|---------|-------------|-------------|
| `models/ex05_spark_model/` | Modèle Spark ML | ✅ Oui |
| `reports/train_metrics.json` | Métriques d'entraînement | ✅ Oui |
| `reports/error_summary.json` | Analyse des erreurs | ❌ Non |
| `reports/error_by_price_bucket.json` | Erreurs par tranche | ❌ Non |

## 🚨 Gestion des Erreurs

L'application gère proprement les cas où les artefacts sont absents :

- **Modèle absent** : Message d'erreur clair, prédiction désactivée
- **Métriques absentes** : Avertissement, section affichée partiellement
- **Rapports d'erreurs absents** : Message informatif, section masquée

## 🛠️ Développement Local (hors Docker)

Si vous souhaitez développer localement :

```bash
# Installation des dépendances
cd ex05_ml_prediction_service/streamlit_app
pip install -r requirements.txt

# Lancement
streamlit run app.py --server.port 8501
```

> ⚠️ Nécessite Java et Spark configurés localement.

## 📊 Données Techniques

- **Framework ML** : Apache Spark ML 3.5
- **Algorithme** : GBTRegressor (Gradient Boosted Trees)
- **Données** : NYC TLC Yellow Taxi (2023)
- **Features** : Distance, durée, zones, heure, jour, paiement

## 📝 Changelog

### v1.0.0 (2026-01)
- Version initiale
- 5 sections complètes
- Intégration Docker
- Gestion des erreurs

## 🤝 Contribution

Pour toute suggestion ou amélioration, contactez l'équipe Data.

---

*Projet NYC Taxi Big Data Pipeline - Exercice EX05*
