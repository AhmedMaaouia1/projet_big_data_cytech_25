"""
Streamlit Application - NYC Taxi ML Model Demonstration
=======================================================

Application professionnelle de démonstration et validation
du modèle de prédiction de tarifs de taxis NYC.

Destinée aux utilisateurs internes (data / métier).
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import (
    StructType, StructField, DoubleType, IntegerType, StringType, TimestampType
)


# =============================================================================
# CONFIGURATION
# =============================================================================

# Paths relatifs depuis le dossier streamlit_app
BASE_DIR = Path(__file__).parent.parent
MODELS_DIR = BASE_DIR / "models"
REPORTS_DIR = BASE_DIR / "reports"
MODEL_PATH = MODELS_DIR / "ex05_spark_model"

# Page config
st.set_page_config(
    page_title="NYC Taxi ML - Démonstration",
    page_icon="T",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E3A5F;
        text-align: center;
        padding: 1rem 0;
        border-bottom: 3px solid #FFD700;
        margin-bottom: 2rem;
    }
    .section-header {
        font-size: 1.5rem;
        font-weight: bold;
        color: #1E3A5F;
        border-left: 5px solid #FFD700;
        padding-left: 1rem;
        margin: 2rem 0 1rem 0;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        border-radius: 10px;
        padding: 1rem;
        color: white;
        text-align: center;
    }
    .prediction-result {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        border-radius: 15px;
        padding: 2rem;
        color: white;
        text-align: center;
        font-size: 2rem;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border: 1px solid #ffc107;
        border-radius: 10px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .info-box {
        background-color: #d1ecf1;
        border: 1px solid #17a2b8;
        border-radius: 10px;
        padding: 1rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border: 1px solid #dc3545;
        border-radius: 10px;
        padding: 1rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

@st.cache_resource
def get_spark_session() -> SparkSession:
    """Créer ou récupérer une session Spark."""
    return (
        SparkSession.builder
        .appName("StreamlitMLDemo")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@st.cache_resource
def load_model() -> Optional[PipelineModel]:
    """Charger le modèle Spark ML pré-entraîné."""
    try:
        if not MODEL_PATH.exists():
            return None
        spark = get_spark_session()
        model = PipelineModel.load(str(MODEL_PATH))
        return model
    except Exception as e:
        st.error(f"Erreur lors du chargement du modèle: {e}")
        return None


def load_json_report(filename: str) -> Optional[dict]:
    """Charger un fichier JSON depuis le dossier reports."""
    filepath = REPORTS_DIR / filename
    if not filepath.exists():
        return None
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        st.warning(f"Impossible de lire {filename}: {e}")
        return None


def format_duration(seconds: float) -> str:
    """Formater une durée en format lisible."""
    if seconds < 60:
        return f"{seconds:.1f} secondes"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} heures"


def format_number(n: float, decimals: int = 2) -> str:
    """Formater un nombre avec séparateurs de milliers."""
    if n >= 1_000_000:
        return f"{n/1_000_000:.{decimals}f}M"
    elif n >= 1_000:
        return f"{n/1_000:.{decimals}f}K"
    return f"{n:.{decimals}f}"


# =============================================================================
# SECTION 1: PRÉSENTATION
# =============================================================================

def render_presentation_section():
    """Afficher la section de présentation du modèle."""
    st.markdown('<div class="main-header">NYC Taxi Fare Prediction — Démonstration ML</div>', 
                unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        ### À propos de ce modèle
        
        Ce modèle de Machine Learning prédit le **tarif total** d'une course de taxi 
        à New York City basé sur les caractéristiques du trajet.
        
        **Données d'entraînement**
        - Source: NYC Taxi & Limousine Commission (TLC)
        - Taxis jaunes (Yellow Cabs) de Manhattan
        - Données historiques 2023
        
        **Architecture technique**
        - Framework: Apache Spark ML
        - Algorithme: **GBTRegressor** (Gradient Boosted Trees)
        - Pipeline: StringIndexer → OneHotEncoder → VectorAssembler → GBT
        
        **Features utilisées**
        - Distance du trajet (miles)
        - Durée du trajet (minutes)  
        - Nombre de passagers
        - Heure et jour de la semaine
        - Zones de prise en charge et dépose
        - Type de paiement
        """)
    
    with col2:
        st.markdown("""
        ### Objectif
        
        Prédire le montant total (`total_amount`) 
        d'une course **sans utiliser** les composantes 
        tarifaires (fare, tips, surcharges).
        
        ---
        
        ### Note importante
        
        Ce modèle est un **outil de démonstration** 
        destiné à valider l'approche ML. Il n'est 
        **pas conçu** pour une utilisation en 
        production temps réel.
        """)
        
        # Status du modèle
        model = load_model()
        if model:
            st.success("Modèle chargé avec succès")
        else:
            st.error("Modèle non disponible")


# =============================================================================
# SECTION 2: PRÉDICTION MANUELLE
# =============================================================================

# Mapping des zones populaires de NYC
NYC_ZONES = {
    1: "Newark Airport",
    4: "Alphabet City",
    7: "Astoria",
    12: "Battery Park",
    13: "Battery Park City",
    24: "Bloomingdale",
    41: "Central Harlem",
    42: "Central Harlem North",
    43: "Central Park",
    45: "Chinatown",
    48: "Clinton East",
    50: "Clinton West",
    68: "East Chelsea",
    74: "East Harlem North",
    75: "East Harlem South",
    79: "East Village",
    87: "Financial District North",
    88: "Financial District South",
    90: "Flatiron",
    100: "Garment District",
    107: "Gramercy",
    113: "Greenwich Village North",
    114: "Greenwich Village South",
    125: "Hudson Sq",
    128: "Jackson Heights",
    132: "JFK Airport",
    137: "Kips Bay",
    138: "LaGuardia Airport",
    140: "Lenox Hill East",
    141: "Lenox Hill West",
    142: "Lincoln Square East",
    143: "Lincoln Square West",
    144: "Little Italy/NoLiTa",
    148: "Lower East Side",
    151: "Manhattan Valley",
    152: "Manhattanville",
    153: "Marble Hill",
    158: "Meatpacking/West Village West",
    161: "Midtown Center",
    162: "Midtown East",
    163: "Midtown North",
    164: "Midtown South",
    166: "Morningside Heights",
    170: "Murray Hill",
    186: "Penn Station/Madison Sq West",
    209: "Seaport",
    211: "SoHo",
    224: "Stuy Town/PCV",
    229: "Sutton Place/Turtle Bay North",
    230: "Sutton Place/Turtle Bay South",
    231: "Times Sq/Theatre District",
    232: "TriBeCa/Civic Center",
    233: "Two Bridges/Seward Park",
    234: "UN/Turtle Bay South",
    236: "Upper East Side North",
    237: "Upper East Side South",
    238: "Upper West Side North",
    239: "Upper West Side South",
    243: "Washington Heights North",
    244: "Washington Heights South",
    246: "West Chelsea/Hudson Yards",
    249: "West Village",
    261: "World Trade Center",
    262: "Yorkville East",
    263: "Yorkville West",
}

PAYMENT_TYPES = {
    1: "Carte de crédit",
    2: "Cash",
    3: "Sans frais",
    4: "Litige",
    5: "Inconnu",
    6: "Trajet annulé"
}

VENDOR_IDS = {
    1: "Creative Mobile Technologies",
    2: "VeriFone Inc."
}

RATE_CODES = {
    1: "Standard",
    2: "JFK",
    3: "Newark",
    4: "Nassau/Westchester",
    5: "Tarif négocié",
    6: "Groupe"
}


def render_prediction_section():
    """Afficher la section de prédiction manuelle."""
    st.markdown('<div class="section-header">Prédiction Manuelle</div>', 
                unsafe_allow_html=True)
    
    model = load_model()
    
    if not model:
        st.markdown("""
        <div class="error-box">
            <strong>Modèle non disponible</strong><br>
            Le modèle Spark ML n'a pas pu être chargé. 
            Vérifiez que le dossier <code>models/ex05_spark_model</code> existe 
            et contient un modèle valide.
        </div>
        """, unsafe_allow_html=True)
        return
    
    st.markdown("""
    Remplissez les caractéristiques du trajet pour obtenir une estimation du tarif.
    """)
    
    # Formulaire de saisie
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.subheader("Trajet")
        trip_distance = st.number_input(
            "Distance (miles)", 
            min_value=0.1, 
            max_value=100.0, 
            value=3.5,
            step=0.1,
            help="Distance du trajet en miles"
        )
        
        trip_duration = st.number_input(
            "Durée estimée (minutes)", 
            min_value=1, 
            max_value=180, 
            value=15,
            help="Durée estimée du trajet"
        )
        
        passenger_count = st.slider(
            "Nombre de passagers", 
            min_value=1, 
            max_value=6, 
            value=1
        )
    
    with col2:
        st.subheader("Horaire")
        pickup_hour = st.slider(
            "Heure de prise en charge", 
            min_value=0, 
            max_value=23, 
            value=14,
            help="Heure de la journée (0-23)"
        )
        
        pickup_day = st.selectbox(
            "Jour de la semaine",
            options=[1, 2, 3, 4, 5, 6, 7],
            format_func=lambda x: ["Dimanche", "Lundi", "Mardi", "Mercredi", 
                                   "Jeudi", "Vendredi", "Samedi"][x-1],
            index=2
        )
        
        pickup_month = st.selectbox(
            "Mois",
            options=list(range(1, 13)),
            format_func=lambda x: ["Janvier", "Février", "Mars", "Avril", 
                                   "Mai", "Juin", "Juillet", "Août",
                                   "Septembre", "Octobre", "Novembre", "Décembre"][x-1],
            index=0
        )
    
    with col3:
        st.subheader("Détails")
        
        # Zones de départ/arrivée
        zone_options = list(NYC_ZONES.keys())
        zone_labels = [f"{k} - {v}" for k, v in NYC_ZONES.items()]
        
        pu_location = st.selectbox(
            "Zone de départ",
            options=zone_options,
            format_func=lambda x: f"{x} - {NYC_ZONES.get(x, 'Zone ' + str(x))}",
            index=zone_options.index(161) if 161 in zone_options else 0
        )
        
        do_location = st.selectbox(
            "Zone d'arrivée",
            options=zone_options,
            format_func=lambda x: f"{x} - {NYC_ZONES.get(x, 'Zone ' + str(x))}",
            index=zone_options.index(132) if 132 in zone_options else 0
        )
        
        payment_type = st.selectbox(
            "Type de paiement",
            options=list(PAYMENT_TYPES.keys()),
            format_func=lambda x: PAYMENT_TYPES[x],
            index=0
        )
    
    # Options avancées
    with st.expander("Options avancées"):
        adv_col1, adv_col2 = st.columns(2)
        with adv_col1:
            vendor_id = st.selectbox(
                "Fournisseur",
                options=list(VENDOR_IDS.keys()),
                format_func=lambda x: VENDOR_IDS[x]
            )
        with adv_col2:
            rate_code = st.selectbox(
                "Code tarifaire",
                options=list(RATE_CODES.keys()),
                format_func=lambda x: RATE_CODES[x]
            )
        store_and_fwd = st.selectbox(
            "Store and Forward",
            options=["N", "Y"],
            help="Indique si le trajet a été stocké avant transmission"
        )
    
    # Bouton de prédiction
    st.markdown("---")
    
    if st.button("Calculer l'estimation", type="primary", use_container_width=True):
        with st.spinner("Calcul de la prédiction..."):
            try:
                # Créer le DataFrame Spark avec les features
                spark = get_spark_session()
                
                # Créer un timestamp fictif pour les features temporelles
                pickup_datetime = datetime(2023, pickup_month, 15, pickup_hour, 0, 0)
                dropoff_datetime = datetime(2023, pickup_month, 15, pickup_hour, 
                                           int(trip_duration), 0)
                
                # Schema pour le DataFrame
                schema = StructType([
                    StructField("VendorID", IntegerType(), True),
                    StructField("tpep_pickup_datetime", TimestampType(), True),
                    StructField("tpep_dropoff_datetime", TimestampType(), True),
                    StructField("passenger_count", DoubleType(), True),
                    StructField("trip_distance", DoubleType(), True),
                    StructField("RatecodeID", DoubleType(), True),
                    StructField("store_and_fwd_flag", StringType(), True),
                    StructField("PULocationID", IntegerType(), True),
                    StructField("DOLocationID", IntegerType(), True),
                    StructField("payment_type", IntegerType(), True),
                    StructField("trip_duration_min", DoubleType(), True),
                    StructField("pickup_hour", IntegerType(), True),
                    StructField("pickup_dayofweek", IntegerType(), True),
                    StructField("pickup_month", IntegerType(), True),
                ])
                
                # Données pour la prédiction
                data = [(
                    vendor_id,
                    pickup_datetime,
                    dropoff_datetime,
                    float(passenger_count),
                    float(trip_distance),
                    float(rate_code),
                    store_and_fwd,
                    pu_location,
                    do_location,
                    payment_type,
                    float(trip_duration),
                    pickup_hour,
                    pickup_day,
                    pickup_month,
                )]
                
                input_df = spark.createDataFrame(data, schema)
                
                # Prédiction
                prediction_df = model.transform(input_df)
                predicted_amount = prediction_df.select("prediction").collect()[0][0]
                
                # Affichage du résultat
                st.markdown(f"""
                <div class="prediction-result">
                    Tarif estimé: <strong>${predicted_amount:.2f}</strong>
                </div>
                """, unsafe_allow_html=True)
                
                # Détails de la prédiction
                st.markdown("#### Détails de l'estimation")
                
                detail_col1, detail_col2, detail_col3 = st.columns(3)
                with detail_col1:
                    st.metric("Distance", f"{trip_distance} miles")
                with detail_col2:
                    st.metric("Durée", f"{trip_duration} min")
                with detail_col3:
                    st.metric("Prix/mile", f"${predicted_amount/trip_distance:.2f}")
                
                # Contexte
                st.markdown("""
                <div class="info-box">
                    <strong>Note:</strong> Cette estimation est basée sur les données 
                    historiques 2023. Les tarifs réels peuvent varier selon les conditions 
                    de trafic, les surcharges et les pourboires.
                </div>
                """, unsafe_allow_html=True)
                
            except Exception as e:
                st.error(f"Erreur lors de la prédiction: {str(e)}")
                st.exception(e)


# =============================================================================
# SECTION 3: QUALITÉ DU MODÈLE
# =============================================================================

def render_quality_section():
    """Afficher la section qualité du modèle."""
    st.markdown('<div class="section-header">Qualité du Modèle</div>', 
                unsafe_allow_html=True)
    
    metrics = load_json_report("train_metrics.json")
    
    if not metrics:
        st.markdown("""
        <div class="warning-box">
            <strong>Métriques non disponibles</strong><br>
            Le fichier <code>reports/train_metrics.json</code> n'a pas été trouvé.
            Exécutez l'entraînement du modèle pour générer ce fichier.
        </div>
        """, unsafe_allow_html=True)
        return
    
    # Métriques principales
    st.subheader("Métriques de Performance")
    
    col1, col2, col3, col4 = st.columns(4)
    
    model_metrics = metrics.get("metrics", {})
    
    with col1:
        rmse = model_metrics.get("rmse", 0)
        st.metric(
            "RMSE",
            f"${rmse:.2f}",
            help="Root Mean Square Error - Erreur quadratique moyenne"
        )
    
    with col2:
        mae = model_metrics.get("mae", 0)
        st.metric(
            "MAE",
            f"${mae:.2f}",
            help="Mean Absolute Error - Erreur absolue moyenne"
        )
    
    with col3:
        r2 = model_metrics.get("r2", 0)
        st.metric(
            "R²",
            f"{r2:.2%}",
            help="Coefficient de détermination - Variance expliquée"
        )
    
    with col4:
        duration = metrics.get("execution", {}).get("duration_seconds", 0)
        st.metric(
            "Temps d'entraînement",
            format_duration(duration)
        )
    
    # Visualisation R²
    st.markdown("---")
    
    col_viz1, col_viz2 = st.columns(2)
    
    with col_viz1:
        st.subheader("Performance R²")
        
        # Gauge chart pour R²
        fig_r2 = go.Figure(go.Indicator(
            mode="gauge+number",
            value=r2 * 100,
            domain={'x': [0, 1], 'y': [0, 1]},
            title={'text': "R² Score (%)"},
            gauge={
                'axis': {'range': [0, 100]},
                'bar': {'color': "#11998e"},
                'steps': [
                    {'range': [0, 50], 'color': "#ff6b6b"},
                    {'range': [50, 75], 'color': "#feca57"},
                    {'range': [75, 90], 'color': "#48dbfb"},
                    {'range': [90, 100], 'color': "#1dd1a1"}
                ],
                'threshold': {
                    'line': {'color': "red", 'width': 4},
                    'thickness': 0.75,
                    'value': 90
                }
            }
        ))
        fig_r2.update_layout(height=300)
        st.plotly_chart(fig_r2, use_container_width=True)
    
    with col_viz2:
        st.subheader("Métriques d'erreur")
        
        # Bar chart pour RMSE et MAE
        error_df = pd.DataFrame({
            'Métrique': ['RMSE', 'MAE'],
            'Valeur ($)': [rmse, mae]
        })
        
        fig_errors = px.bar(
            error_df,
            x='Métrique',
            y='Valeur ($)',
            color='Métrique',
            color_discrete_map={'RMSE': '#667eea', 'MAE': '#764ba2'}
        )
        fig_errors.update_layout(height=300, showlegend=False)
        st.plotly_chart(fig_errors, use_container_width=True)
    
    # Informations sur les données
    st.markdown("---")
    st.subheader("Données d'Entraînement")
    
    data_info = metrics.get("data", {})
    params = metrics.get("params", {})
    
    info_col1, info_col2, info_col3 = st.columns(3)
    
    with info_col1:
        st.markdown("**Période des données**")
        months = data_info.get("months", [])
        st.write(", ".join(months) if months else "Non spécifié")
        
    with info_col2:
        st.markdown("**Volume de données**")
        train_rows = data_info.get("train_rows", 0)
        test_rows = data_info.get("test_rows", 0)
        st.write(f"Train: {format_number(train_rows)} lignes")
        st.write(f"Test: {format_number(test_rows)} lignes")
        
    with info_col3:
        st.markdown("**Hyperparamètres**")
        st.write(f"Max Depth: {params.get('maxDepth', 'N/A')}")
        st.write(f"Max Iterations: {params.get('maxIter', 'N/A')}")
        st.write(f"Seed: {params.get('seed', 'N/A')}")
    
    # Timestamp
    timestamp = metrics.get("timestamp", "")
    if timestamp:
        st.caption(f"Entraînement effectué le: {timestamp[:19].replace('T', ' ')}")


# =============================================================================
# SECTION 4: ANALYSE D'ERREURS
# =============================================================================

def render_error_analysis_section():
    """Afficher la section d'analyse des erreurs."""
    st.markdown('<div class="section-header">Analyse des Erreurs</div>', 
                unsafe_allow_html=True)
    
    # Charger les rapports d'erreur
    error_summary = load_json_report("error_summary.json")
    error_buckets = load_json_report("error_by_price_bucket.json")
    train_metrics = load_json_report("train_metrics.json")
    
    # Si les fichiers dédiés n'existent pas, essayer de récupérer depuis train_metrics
    if not error_summary and train_metrics:
        error_analysis = train_metrics.get("error_analysis", {})
        if error_analysis:
            error_summary = {
                "summary": error_analysis.get("summary", {}),
                "business_insights": error_analysis.get("business_insights", [])
            }
    
    if not error_summary and not error_buckets:
        st.markdown("""
        <div class="warning-box">
            <strong>Analyse d'erreurs non disponible</strong><br>
            Les fichiers d'analyse d'erreurs n'ont pas été trouvés.
            Exécutez l'entraînement du modèle pour générer ces rapports.
        </div>
        """, unsafe_allow_html=True)
        return
    
    # Résumé global des erreurs
    if error_summary:
        summary = error_summary.get("summary", {})
        
        st.subheader("Distribution des Erreurs")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                "Erreur moyenne",
                f"${summary.get('mean_error', 0):.2f}",
                help="Moyenne des erreurs (prédiction - réel)"
            )
        
        with col2:
            st.metric(
                "Erreur médiane",
                f"${summary.get('median_error', 0):.2f}",
                help="Médiane des erreurs"
            )
        
        with col3:
            st.metric(
                "Sous-estimation",
                f"{summary.get('pct_underestimate', 0):.1f}%",
                help="% de cas où le modèle sous-estime"
            )
        
        with col4:
            st.metric(
                "Sur-estimation", 
                f"{summary.get('pct_overestimate', 0):.1f}%",
                help="% de cas où le modèle sur-estime"
            )
        
        # Visualisation du biais
        st.markdown("---")
        
        col_bias1, col_bias2 = st.columns(2)
        
        with col_bias1:
            st.subheader("Biais du Modèle")
            
            under = summary.get('pct_underestimate', 50)
            over = summary.get('pct_overestimate', 50)
            
            fig_bias = go.Figure(data=[
                go.Pie(
                    labels=['Sous-estimation', 'Sur-estimation'],
                    values=[under, over],
                    hole=0.4,
                    marker_colors=['#ff6b6b', '#48dbfb']
                )
            ])
            fig_bias.update_layout(height=300)
            st.plotly_chart(fig_bias, use_container_width=True)
        
        with col_bias2:
            st.subheader("Percentiles d'Erreur")
            
            percentiles_df = pd.DataFrame({
                'Percentile': ['P25', 'P50 (Médiane)', 'P75', 'P95', 'P99'],
                'Erreur ($)': [
                    summary.get('p25_error', 0),
                    summary.get('median_error', 0),
                    summary.get('p75_error', 0),
                    summary.get('p95_error', 0),
                    summary.get('p99_error', 0),
                ]
            })
            
            fig_perc = px.bar(
                percentiles_df,
                x='Percentile',
                y='Erreur ($)',
                color='Erreur ($)',
                color_continuous_scale='RdYlGn_r'
            )
            fig_perc.update_layout(height=300, showlegend=False)
            st.plotly_chart(fig_perc, use_container_width=True)
    
    # Erreurs par tranche de prix
    if error_buckets:
        st.markdown("---")
        st.subheader("Erreurs par Tranche de Prix")
        
        buckets = error_buckets.get("buckets", [])
        
        if buckets:
            buckets_df = pd.DataFrame(buckets)
            
            # Renommer pour affichage
            bucket_names_fr = {
                'low': '< $10 (Bas)',
                'medium': '$10-30 (Moyen)',
                'high': '$30-60 (Élevé)',
                'very_high': '> $60 (Très élevé)'
            }
            buckets_df['bucket_label'] = buckets_df['bucket_name'].map(bucket_names_fr)
            
            col_buck1, col_buck2 = st.columns(2)
            
            with col_buck1:
                st.markdown("**Distribution des trajets**")
                fig_dist = px.pie(
                    buckets_df,
                    names='bucket_label',
                    values='pct_of_total',
                    color='bucket_name',
                    color_discrete_map={
                        'low': '#1dd1a1',
                        'medium': '#48dbfb', 
                        'high': '#feca57',
                        'very_high': '#ff6b6b'
                    }
                )
                fig_dist.update_layout(height=350)
                st.plotly_chart(fig_dist, use_container_width=True)
            
            with col_buck2:
                st.markdown("**MAE par tranche de prix**")
                fig_mae = px.bar(
                    buckets_df,
                    x='bucket_label',
                    y='mean_abs_error',
                    color='mean_abs_error',
                    color_continuous_scale='Reds'
                )
                fig_mae.update_layout(
                    height=350,
                    xaxis_title="Tranche de prix",
                    yaxis_title="MAE ($)",
                    showlegend=False
                )
                st.plotly_chart(fig_mae, use_container_width=True)
            
            # Tableau détaillé
            st.markdown("**Tableau détaillé**")
            display_df = buckets_df[['bucket_label', 'count', 'pct_of_total', 
                                     'mean_error', 'mean_abs_error', 'rmse']].copy()
            display_df.columns = ['Tranche', 'Nb trajets', '% du total', 
                                 'Erreur moy.', 'MAE', 'RMSE']
            st.dataframe(display_df, use_container_width=True, hide_index=True)
    
    # Insights métier
    if error_summary:
        insights = error_summary.get("business_insights", [])
        
        if insights:
            st.markdown("---")
            st.subheader("Insights Métier")
            
            for insight in insights:
                st.markdown(f"""
                <div class="info-box">
                    {insight}
                </div>
                """, unsafe_allow_html=True)


# =============================================================================
# SECTION 5: LIMITES ET CADRE D'UTILISATION
# =============================================================================

def render_limitations_section():
    """Afficher la section sur les limites et le cadre d'utilisation."""
    st.markdown('<div class="section-header">Limites & Cadre d\'Utilisation</div>', 
                unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        ### Limitations du Modèle
        
        **1. Données historiques uniquement**
        - Le modèle est entraîné sur des données de janvier-février 2023
        - Il ne capture pas les évolutions tarifaires récentes
        - Les événements exceptionnels ne sont pas pris en compte
        
        **2. Pas de temps réel**
        - Pas d'intégration avec les conditions de trafic actuelles
        - Pas de prise en compte de la météo
        - Pas d'ajustement pour les événements spéciaux
        
        **3. Biais potentiels**
        - Sur-représentation de certaines zones (Manhattan)
        - Données potentiellement incomplètes (cash, tips)
        - Tarifs négociés non modélisables
        
        **4. Cas particuliers non couverts**
        - Tarifs forfaitaires (aéroports)
        - Courses longue distance hors NYC
        - Partage de course (ride-sharing)
        """)
    
    with col2:
        st.markdown("""
        ### Cas d'usage appropriés
        
        **Cet outil est conçu pour:**
        
        - **Démonstration** des capacités ML
        - **Validation** de l'approche prédictive  
        - **Analyse** de la distribution des tarifs
        - **Formation** aux techniques ML/Spark
        - **Tests** de pipelines de données
        
        ---
        
        ### Public cible
        
        - Data Scientists / Data Engineers
        - Équipes métier (analyse tarifaire)
        - Équipes produit (évaluation de faisabilité)
        
        ---
        
        ### À retenir
        
        > Ce modèle est un **proof of concept** démontrant 
        > la faisabilité de la prédiction de tarifs avec 
        > Spark ML. Il ne doit **pas** être utilisé pour 
        > des décisions commerciales ou une mise en production.
        """)
    
    # Encadré final
    st.markdown("---")
    
    st.markdown("""
    <div class="warning-box">
        <h4>Résumé</h4>
        <ul>
            <li><strong>Type:</strong> Modèle de démonstration / Proof of Concept</li>
            <li><strong>Usage:</strong> Interne uniquement (équipes data/métier)</li>
            <li><strong>Données:</strong> NYC TLC Yellow Taxi - 2023</li>
            <li><strong>Framework:</strong> Apache Spark ML (GBTRegressor)</li>
            <li><strong>Limitations:</strong> Pas de temps réel, données historiques uniquement</li>
        </ul>
    </div>
    """, unsafe_allow_html=True)


# =============================================================================
# MAIN APPLICATION
# =============================================================================

def main():
    """Point d'entrée principal de l'application."""
    
    # Section 1: Présentation
    render_presentation_section()
    
    st.markdown("---")
    
    # Section 2: Prédiction manuelle
    render_prediction_section()
    
    st.markdown("---")
    
    # Section 3: Qualité du modèle
    render_quality_section()
    
    st.markdown("---")
    
    # Section 4: Analyse d'erreurs
    render_error_analysis_section()
    
    st.markdown("---")
    
    # Section 5: Limites
    render_limitations_section()
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #666; padding: 1rem;">
        <small>
            NYC Taxi ML Prediction Demo — Projet Big Data Pipeline — Streamlit & Spark ML
        </small>
    </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
