import os
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --------------------------------------------------
# Configuration générale
# --------------------------------------------------
st.set_page_config(
    page_title="NYC Yellow Taxi Analytics",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS pour améliorer le design
st.markdown("""
<style>
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a1a2e 0%, #16213e 100%);
        border-right: none;
    }
    
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h2,
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h3 {
        color: #ffffff !important;
        font-size: 0.9rem !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-top: 1.5rem !important;
        margin-bottom: 0.5rem !important;
    }
    
    [data-testid="stSidebar"] label {
        color: #e0e0e0 !important;
        font-size: 0.85rem !important;
    }
    
    [data-testid="stSidebar"] input {
        background-color: #0f3460 !important;
        color: white !important;
        border: 1px solid #16213e !important;
    }
    
    /* Main content styling */
    .main {
        background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
    }
    
    /* Metric cards */
    [data-testid="stMetric"] {
        background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
        padding: 1.2rem;
        border-radius: 12px;
        border: 1px solid #334155;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
    }
    
    [data-testid="stMetricValue"] {
        font-size: 2rem;
        font-weight: 700;
        color: #ffffff;
    }
    
    [data-testid="stMetricLabel"] {
        color: #94a3b8 !important;
        font-size: 0.875rem;
    }
    
    /* Headers */
    h1 {
        color: #ffffff;
        font-weight: 700;
        padding-bottom: 1rem;
    }
    
    h2, h3 {
        color: #e2e8f0;
        font-weight: 600;
    }
    
    p {
        color: #cbd5e1;
    }
    

    /* Dataframe styling */
    [data-testid="stDataFrame"] {
        background: #1e293b;
        border-radius: 12px;
        padding: 1rem;
        border: 1px solid #334155;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
    }
    
    /* Charts background */
    .js-plotly-plot {
        background-color: transparent !important;
    }
    
    /* Multiselect styling */
    [data-testid="stMultiSelect"] span[data-baseweb="tag"] {
        background-color: #3b82f6 !important;
    }
    
    /* Selectbox styling */
    [data-baseweb="select"] {
        background-color: #0f3460 !important;
    }
    
    /* Date input visibility */
    [data-testid="stSidebar"] [data-testid="stDateInput"] {
        margin-bottom: 1rem;
    }
    
    /* Divider */
    hr {
        border-color: #334155 !important;
    }
</style>
""", unsafe_allow_html=True)

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

engine = create_engine(
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
    pool_pre_ping=True
)

@st.cache_data(ttl=300)
def read_sql(query, params=None):
    with engine.connect() as conn:
        return pd.read_sql(text(query), conn, params=params or {})


# --------------------------------------------------
# Période disponible (dynamique)
# --------------------------------------------------
period = read_sql("""
SELECT
    MIN(pickup_date) AS min_date,
    MAX(pickup_date) AS max_date
FROM fact_trip;
""").iloc[0]

# --------------------------------------------------
# Sidebar – Filtres globaux
# --------------------------------------------------
st.sidebar.markdown("## Filtres")

# Filtre période
st.sidebar.markdown("### Période")
start_date, end_date = st.sidebar.date_input(
    "Sélectionnez la période d'analyse",
    value=(period.min_date, period.max_date),
    min_value=period.min_date,
    max_value=period.max_date
)

# Filtre type de paiement
payment_types = read_sql("""
SELECT payment_description
FROM dim_payment_type
ORDER BY payment_description;
""")["payment_description"].tolist()

st.sidebar.markdown("### Type de paiement")

selected_payments = st.sidebar.multiselect(
    "Sélectionner les types de paiement",
    options=payment_types,
    default=payment_types
)

# Filtre borough
boroughs = read_sql("""
SELECT DISTINCT borough
FROM dim_location
ORDER BY borough;
""")["borough"].dropna().tolist()

st.sidebar.markdown("### Arrondissement")
selected_boroughs = st.sidebar.multiselect(
    "Sélectionner les arrondissements",
    options=boroughs,
    default=boroughs
)

# Filtre zone
zones = read_sql("""
SELECT DISTINCT zone
FROM dim_location
WHERE borough = ANY(:boroughs)
ORDER BY zone;
""", {"boroughs": selected_boroughs})["zone"].dropna().tolist()

st.sidebar.markdown("### Zone")

selected_zones = st.sidebar.multiselect(
    "Sélectionner les zones",
    options=zones,
    default=zones
)

# Paramètres SQL communs
params = {
    "start_date": start_date,
    "end_date": end_date,
    "payment_types": selected_payments,
    "boroughs": selected_boroughs,
    "zones": selected_zones
}

# --------------------------------------------------
# Header
# --------------------------------------------------
st.title("NYC Yellow Taxi Analytics")
st.markdown(
    f"**Période d'analyse :** {start_date.strftime('%d %b %Y')} → {end_date.strftime('%d %b %Y')}"
)

st.markdown("---")

# --------------------------------------------------
# KPIs globaux
# --------------------------------------------------
kpi = read_sql("""
SELECT
    COUNT(*) AS trips,
    SUM(f.total_amount) AS revenue,
    AVG(f.total_amount) AS avg_amount,
    AVG(f.trip_distance) AS avg_distance
FROM fact_trip f
JOIN dim_payment_type p ON f.payment_type_id = p.payment_type_id
JOIN dim_location l ON f.pickup_location_id = l.location_id
WHERE f.pickup_date BETWEEN :start_date AND :end_date
  AND p.payment_description = ANY(:payment_types)
  AND l.borough = ANY(:boroughs)
  AND l.zone = ANY(:zones);
""", params).iloc[0]

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Total des courses",
        value=f"{int(kpi.trips):,}"
    )

with col2:
    st.metric(
        label="Chiffre d'affaires",
        value=f"${kpi.revenue:,.0f}"
    )

with col3:
    st.metric(
        label="Montant moyen",
        value=f"${kpi.avg_amount:.2f}"
    )

with col4:
    st.metric(
        label="Distance moyenne",
        value=f"{kpi.avg_distance:.2f} mi"
    )

st.markdown("<br>", unsafe_allow_html=True)

# --------------------------------------------------
# Évolution quotidienne
# --------------------------------------------------
st.markdown("### Évolution quotidienne des courses")

df_daily = read_sql("""
SELECT
    f.pickup_date,
    COUNT(*) AS trips
FROM fact_trip f
JOIN dim_payment_type p ON f.payment_type_id = p.payment_type_id
JOIN dim_location l ON f.pickup_location_id = l.location_id
WHERE f.pickup_date BETWEEN :start_date AND :end_date
  AND p.payment_description = ANY(:payment_types)
  AND l.borough = ANY(:boroughs)
  AND l.zone = ANY(:zones)
GROUP BY f.pickup_date
ORDER BY f.pickup_date;
""", params)

fig_daily = px.area(
    df_daily,
    x="pickup_date",
    y="trips",
    labels={"pickup_date": "Date", "trips": "Nombre de courses"},
    color_discrete_sequence=["#3b82f6"]
)
fig_daily.update_layout(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(30,41,59,0.5)",
    xaxis=dict(showgrid=True, gridcolor="#334155", color="#e2e8f0"),
    yaxis=dict(showgrid=True, gridcolor="#334155", color="#e2e8f0"),
    hovermode="x unified",
    font=dict(color="#e2e8f0")
)
st.plotly_chart(fig_daily, use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# --------------------------------------------------
# Graphiques en colonnes
# --------------------------------------------------
col_left, col_right = st.columns(2)

# Heures de pointe
with col_left:
    st.markdown("### Heures de pointe")
    
    df_hourly = read_sql("""
    SELECT
        t.hour,
        COUNT(*) AS trips
    FROM fact_trip f
    JOIN dim_time t ON f.pickup_time = t.time_id
    JOIN dim_payment_type p ON f.payment_type_id = p.payment_type_id
    JOIN dim_location l ON f.pickup_location_id = l.location_id
    WHERE f.pickup_date BETWEEN :start_date AND :end_date
      AND p.payment_description = ANY(:payment_types)
      AND l.borough = ANY(:boroughs)
      AND l.zone = ANY(:zones)
    GROUP BY t.hour
    ORDER BY t.hour;
    """, params)
    
    fig_hourly = px.bar(
        df_hourly,
        x="hour",
        y="trips",
        labels={"hour": "Heure", "trips": "Courses"},
        color="trips",
        color_continuous_scale=["#1e40af", "#3b82f6", "#60a5fa"]
    )
    fig_hourly.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(30,41,59,0.5)",
        xaxis=dict(showgrid=False, color="#e2e8f0"),
        yaxis=dict(showgrid=True, gridcolor="#334155", color="#e2e8f0"),
        showlegend=False,
        font=dict(color="#e2e8f0")
    )
    st.plotly_chart(fig_hourly, use_container_width=True)

# Répartition par type de paiement
with col_right:
    st.markdown("### Répartition par type de paiement")
    
    df_payment = read_sql("""
    SELECT
        p.payment_description,
        COUNT(*) AS trips
    FROM fact_trip f
    JOIN dim_payment_type p ON f.payment_type_id = p.payment_type_id
    JOIN dim_location l ON f.pickup_location_id = l.location_id
    WHERE f.pickup_date BETWEEN :start_date AND :end_date
      AND l.borough = ANY(:boroughs)
      AND l.zone = ANY(:zones)
    GROUP BY p.payment_description
    ORDER BY trips DESC;
    """, params)
    
    fig_payment = px.pie(
        df_payment,
        values="trips",
        names="payment_description",
        hole=0.4,
        color_discrete_sequence=["#1b6157", "#9f4653", "#444c91", "#95541e", "#9f9c9b"]
    )
    fig_payment.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(30,41,59,0.5)",
        font=dict(color="#e2e8f0")
    )
    st.plotly_chart(fig_payment, use_container_width=True)

st.markdown("<br>", unsafe_allow_html=True)

# --------------------------------------------------
# Top zones de départ
# --------------------------------------------------
st.markdown("### Top 10 des zones de départ les plus actives")

df_zones = read_sql("""
SELECT
    l.borough AS "Arrondissement",
    l.zone AS "Zone",
    COUNT(*) AS "Courses"
FROM fact_trip f
JOIN dim_location l ON f.pickup_location_id = l.location_id
JOIN dim_payment_type p ON f.payment_type_id = p.payment_type_id
WHERE f.pickup_date BETWEEN :start_date AND :end_date
  AND p.payment_description = ANY(:payment_types)
  AND l.borough = ANY(:boroughs)
  AND l.zone = ANY(:zones)
GROUP BY l.borough, l.zone
ORDER BY "Courses" DESC
LIMIT 10;
""", params)

# Ajouter une colonne de rang
df_zones.insert(0, "Rang", range(1, len(df_zones) + 1))

# Formater le nombre de courses
df_zones["Courses"] = df_zones["Courses"].apply(lambda x: f"{x:,}")

st.dataframe(
    df_zones,
    width="stretch",
    hide_index=True
)