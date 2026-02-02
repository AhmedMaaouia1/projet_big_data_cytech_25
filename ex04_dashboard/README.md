# Exercice 04 – Dashboard & Analytical Consumption

## Objectif

L'exercice **EX04** a pour objectif de fournir une **interface de visualisation interactive** pour exploiter les données du Data Warehouse. Il comprend :

1. **Notebook EDA** : Exploration et analyse des données (Jupyter)
2. **Dashboard Streamlit** : Interface BI interactive pour l'analyse métier

## Architecture

```
┌────────────────────────────────────────────────────────┐
│                   DATA WAREHOUSE                       │
│                    (PostgreSQL)                        │
│                                                        │
│  fact_trip + dim_date + dim_time + dim_location + ... │
└────────────────────────┬───────────────────────────────┘
                         │
            ┌────────────┴────────────┐
            │                         │
            ▼                         ▼
    ┌───────────────┐        ┌───────────────┐
    │ Jupyter NB    │        │   Streamlit   │
    │ (EDA)         │        │  (Dashboard)  │
    └───────────────┘        └───────────────┘
```

## Structure du projet

```
ex04_dashboard/
├── notebooks/
│   ├── ex04_eda.ipynb     # Notebook d'exploration
│   └── ex04_eda.html      # Export HTML
└── streamlit_app/
    ├── app.py             # Application Streamlit
    ├── requirements.txt   # Dépendances Python
    └── .env               # Configuration (non versionné)
```

## Dashboard Streamlit

### Fonctionnalités

Le dashboard offre une vue complète des données NYC Yellow Taxi avec :

#### KPIs principaux
- 📊 **Total des courses** : nombre de trajets
- 💰 **Chiffre d'affaires** : somme des montants totaux
- 📈 **Montant moyen** : moyenne par trajet
- 📏 **Distance moyenne** : en miles

#### Visualisations
- 📅 **Évolution quotidienne** : graphique des courses par jour
- ⏰ **Heures de pointe** : répartition horaire des courses
- 💳 **Types de paiement** : répartition (pie chart)
- 🗺️ **Top 10 zones** : zones de départ les plus actives

#### Filtres interactifs
- 📆 **Période** : sélection de la plage de dates
- 💳 **Type de paiement** : filtrage multi-sélection
- 🏙️ **Arrondissement** : filtrage par borough
- 📍 **Zone** : filtrage par zone TLC

### Technologies

| Composant       | Technologie        |
|-----------------|--------------------|
| Framework       | Streamlit          |
| Visualisation   | Plotly Express     |
| Base de données | PostgreSQL         |
| ORM             | SQLAlchemy         |
| Style           | CSS personnalisé   |

### Configuration requise

Créer un fichier `.env` dans `streamlit_app/` :

```env
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=nyc_dw
POSTGRES_USER=nyc_user
POSTGRES_PASSWORD=nyc_password
```

### Installation

```bash
cd ex04_dashboard/streamlit_app

# Créer environnement virtuel
python -m venv .venv
.venv\Scripts\activate  # Windows
# source .venv/bin/activate  # Linux/Mac

# Installer les dépendances
pip install -r requirements.txt
```

### Dépendances

```
streamlit
pandas
plotly
sqlalchemy
psycopg2-binary
python-dotenv
```

### Exécution

```bash
cd ex04_dashboard/streamlit_app
streamlit run app.py
```

Le dashboard est accessible sur : **http://localhost:8501**

## Notebook EDA

### Contenu

Le notebook `ex04_eda.ipynb` contient :

1. **Connexion** au Data Warehouse
2. **Statistiques descriptives** des trajets
3. **Analyse temporelle** (distribution par jour/heure)
4. **Analyse géographique** (top zones)
5. **Analyse des montants** (distribution, outliers)
6. **Corrélations** entre variables

### Exécution

```bash
cd ex04_dashboard/notebooks
jupyter notebook ex04_eda.ipynb
```

## Requêtes SQL utilisées

### KPIs globaux
```sql
SELECT
    COUNT(*) AS trips,
    SUM(total_amount) AS revenue,
    AVG(total_amount) AS avg_amount,
    AVG(trip_distance) AS avg_distance
FROM fact_trip f
JOIN dim_payment_type p ON f.payment_type_id = p.payment_type_id
JOIN dim_location l ON f.pickup_location_id = l.location_id
WHERE f.pickup_date BETWEEN :start_date AND :end_date;
```

### Évolution quotidienne
```sql
SELECT pickup_date, COUNT(*) AS trips
FROM fact_trip
GROUP BY pickup_date
ORDER BY pickup_date;
```

### Heures de pointe
```sql
SELECT t.hour, COUNT(*) AS trips
FROM fact_trip f
JOIN dim_time t ON f.pickup_time = t.time_id
GROUP BY t.hour
ORDER BY t.hour;
```

### Top zones
```sql
SELECT l.borough, l.zone, COUNT(*) AS trips
FROM fact_trip f
JOIN dim_location l ON f.pickup_location_id = l.location_id
GROUP BY l.borough, l.zone
ORDER BY trips DESC
LIMIT 10;
```

## Design

Le dashboard utilise un thème sombre moderne avec :
- 🎨 Gradient background (#0f172a → #1e293b)
- 📊 Cartes métriques stylisées
- 🔵 Palette de couleurs cohérente
- 📱 Layout responsive

## Captures d'écran

*Le dashboard affiche les KPIs, graphiques temporels, distribution des paiements et classement des zones.*
---
![Architecture](../Documents/Captures/Dataviz/Dataviz1.png)
![Architecture](../Documents/Captures/Dataviz/Dataviz2.png)

---

## Intégration Airflow (EX06)

L'exercice EX04 (Dashboard) n'est **pas orchestré** par Airflow car il s'agit d'une application interactive de visualisation, pas d'un job batch.

Cependant, les données affichées dans le dashboard sont alimentées automatiquement par le pipeline orchestré :

```
EX01 → EX02 → EX03 → Data Warehouse → Dashboard (EX04)
         ↓
        EX05 (ML)
```

**Dépendance :**
- Le dashboard lit les données depuis `fact_trip` et les dimensions
- Ces tables sont alimentées mensuellement par le DAG `full_nyc_taxi_pipeline`
- Les nouvelles données sont automatiquement visibles dans le dashboard après chaque exécution du pipeline

---

## Statut

✅ **Terminé et validé**

---

**Auteur :** MAAOUIA Ahmed – CY Tech Big Data
