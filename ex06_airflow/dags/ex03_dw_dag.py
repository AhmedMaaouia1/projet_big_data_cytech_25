"""
============================================================================
DAG EX03 - Data Warehouse Loading (Chargement DW depuis Staging)
============================================================================
Ce DAG orchestre le chargement incrémental du Data Warehouse PostgreSQL
à partir des données staging alimentées par EX02.

Fonctionnement:
- Exécute les scripts SQL d'insertion depuis staging vers les dimensions et fact_trip
- Utilise ON CONFLICT DO NOTHING pour l'idempotence

Architecture cible:
  staging.yellow_trips_staging  →  dim_date, dim_time, dim_location, ...
                                →  fact_trip

Prérequis:
- EX02 doit avoir été exécuté (staging alimenté)
- Les tables DW doivent exister (dw_creation.sql exécuté une fois)

Idempotence:
- ON CONFLICT DO NOTHING sur les dimensions
- ON CONFLICT DO NOTHING sur fact_trip (clé composite)

Auteur: Pipeline NYC Taxi - CY Tech Big Data
============================================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import os


# ============================================================================
# CONFIGURATION DU DAG
# ============================================================================

default_args = {
    'owner': 'nyc-taxi-pipeline',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    dag_id='ex03_dw_loading',
    default_args=default_args,
    description='EX03 - Chargement incrémental du Data Warehouse depuis staging',
    schedule_interval='@monthly',
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2024, 12, 31),
    catchup=True,
    max_active_runs=1,
    tags=['ex03', 'dw', 'postgres', 'sql'],
    doc_md=__doc__,
)


# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

def log_execution_params(**context):
    """Log les paramètres d'exécution pour le debugging."""
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    
    print("=" * 60)
    print("📊 PARAMÈTRES D'EXÉCUTION EX03")
    print("=" * 60)
    print(f"   Execution Date : {execution_date}")
    print(f"   Année          : {year}")
    print(f"   Mois           : {month:02d}")
    print(f"   Source         : staging.yellow_trips_staging")
    print(f"   Destination    : Data Warehouse (fact_trip + dimensions)")
    print("=" * 60)


# ============================================================================
# DÉFINITION DES TÂCHES
# ============================================================================

with dag:
    
    # Task 1: Attente de la fin de EX02 pour le même mois
    wait_for_ex02 = ExternalTaskSensor(
        task_id='wait_for_ex02_completion',
        external_dag_id='ex02_data_ingestion',
        external_task_id='verify_postgres_staging',
        mode='poke',
        timeout=3600,
        poke_interval=60,
        allowed_states=['success'],
    )
    
    # Task 2: Log des paramètres
    log_params = PythonOperator(
        task_id='log_execution_params',
        python_callable=log_execution_params,
        provide_context=True,
    )
    
    # Task 3: Vérification données staging disponibles
    check_staging = BashOperator(
        task_id='check_staging_data',
        bash_command="""
            echo "🔍 Vérification données staging pour {{ execution_date.year }}-{{ execution_date.strftime('%m') }}..."
            STAGING_COUNT=$(docker exec postgres psql -U ${POSTGRES_USER:-postgres} -d ${POSTGRES_DB:-nyc_taxi} -t -c \
                "SELECT COUNT(*) FROM yellow_trips_staging WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = {{ execution_date.year }} AND EXTRACT(MONTH FROM tpep_pickup_datetime) = {{ execution_date.month }};" 2>/dev/null | tr -d ' ')
            
            if [ -z "$STAGING_COUNT" ] || [ "$STAGING_COUNT" -eq 0 ]; then
                echo "❌ Aucune donnée staging pour ce mois"
                exit 1
            fi
            
            echo "✅ $STAGING_COUNT enregistrements en staging"
        """,
    )
    
    # Task 4: Chargement des dimensions
    # Utilise le script SQL existant avec ON CONFLICT DO NOTHING
    load_dimensions = BashOperator(
        task_id='load_dimensions',
        bash_command="""
            echo "📊 Chargement des dimensions depuis staging..."
            
            docker exec -i postgres psql -U ${POSTGRES_USER:-postgres} -d ${POSTGRES_DB:-nyc_taxi} << 'EOSQL'
            
            -- Chargement dim_vendor
            INSERT INTO dim_vendor (vendor_id)
            SELECT DISTINCT vendorid
            FROM yellow_trips_staging
            WHERE vendorid IS NOT NULL
            ON CONFLICT DO NOTHING;
            
            -- Chargement dim_payment_type  
            INSERT INTO dim_payment_type (payment_type_id)
            SELECT DISTINCT payment_type
            FROM yellow_trips_staging
            WHERE payment_type IS NOT NULL
            ON CONFLICT DO NOTHING;
            
            -- Chargement dim_ratecode
            INSERT INTO dim_ratecode (ratecode_id)
            SELECT DISTINCT ratecodeid
            FROM yellow_trips_staging
            WHERE ratecodeid IS NOT NULL
            ON CONFLICT DO NOTHING;
            
            -- Chargement dim_location (pickup)
            INSERT INTO dim_location (location_id)
            SELECT DISTINCT pulocationid
            FROM yellow_trips_staging
            WHERE pulocationid IS NOT NULL
            ON CONFLICT DO NOTHING;
            
            -- Chargement dim_location (dropoff)
            INSERT INTO dim_location (location_id)
            SELECT DISTINCT dolocationid
            FROM yellow_trips_staging
            WHERE dolocationid IS NOT NULL
            ON CONFLICT DO NOTHING;
            
            -- Chargement dim_date
            INSERT INTO dim_date (date_id, year, month, day, day_of_week)
            SELECT DISTINCT
              DATE(tpep_pickup_datetime),
              EXTRACT(YEAR FROM tpep_pickup_datetime)::INTEGER,
              EXTRACT(MONTH FROM tpep_pickup_datetime)::INTEGER,
              EXTRACT(DAY FROM tpep_pickup_datetime)::INTEGER,
              EXTRACT(DOW FROM tpep_pickup_datetime)::INTEGER
            FROM yellow_trips_staging
            ON CONFLICT DO NOTHING;
            
            -- Chargement dim_time
            INSERT INTO dim_time (time_id, hour, minute)
            SELECT DISTINCT
              CAST(tpep_pickup_datetime AS TIME),
              EXTRACT(HOUR FROM tpep_pickup_datetime)::INTEGER,
              EXTRACT(MINUTE FROM tpep_pickup_datetime)::INTEGER
            FROM yellow_trips_staging
            ON CONFLICT DO NOTHING;
            
EOSQL
            
            echo "✅ Dimensions chargées"
        """,
    )
    
    # Task 5: Chargement de la table de faits
    load_facts = BashOperator(
        task_id='load_fact_trip',
        bash_command="""
            echo "📊 Chargement de fact_trip depuis staging..."
            
            docker exec -i postgres psql -U ${POSTGRES_USER:-postgres} -d ${POSTGRES_DB:-nyc_taxi} << 'EOSQL'
            
            INSERT INTO fact_trip (
              pickup_date,
              pickup_time,
              pickup_location_id,
              dropoff_location_id,
              vendor_id,
              payment_type_id,
              ratecode_id,
              passenger_count,
              trip_distance,
              fare_amount,
              extra,
              mta_tax,
              tip_amount,
              tolls_amount,
              improvement_surcharge,
              congestion_surcharge,
              airport_fee,
              total_amount
            )
            SELECT
              DATE(tpep_pickup_datetime),
              CAST(tpep_pickup_datetime AS TIME),
              pulocationid,
              dolocationid,
              vendorid,
              payment_type,
              ratecodeid,
              passenger_count,
              trip_distance,
              fare_amount,
              extra,
              mta_tax,
              tip_amount,
              tolls_amount,
              improvement_surcharge,
              congestion_surcharge,
              airport_fee,
              total_amount
            FROM yellow_trips_staging
            ON CONFLICT DO NOTHING;
            
EOSQL
            
            echo "✅ Fact table chargée"
        """,
    )
    
    # Task 6: Vérification et comptage final
    verify_dw = BashOperator(
        task_id='verify_dw_loading',
        bash_command="""
            echo "📊 Vérification du chargement DW..."
            
            docker exec postgres psql -U ${POSTGRES_USER:-postgres} -d ${POSTGRES_DB:-nyc_taxi} -c "
                SELECT 'fact_trip' as table_name, COUNT(*) as row_count FROM fact_trip
                UNION ALL
                SELECT 'dim_date', COUNT(*) FROM dim_date
                UNION ALL
                SELECT 'dim_time', COUNT(*) FROM dim_time
                UNION ALL
                SELECT 'dim_location', COUNT(*) FROM dim_location
                UNION ALL
                SELECT 'dim_vendor', COUNT(*) FROM dim_vendor
                UNION ALL
                SELECT 'dim_payment_type', COUNT(*) FROM dim_payment_type
                UNION ALL
                SELECT 'dim_ratecode', COUNT(*) FROM dim_ratecode
                ORDER BY table_name;
            "
            
            echo "✅ EX03 terminé avec succès"
        """,
    )
    
    # Task 7: Nettoyage staging (optionnel - commenté par défaut)
    # Pour éviter l'accumulation de données staging
    # cleanup_staging = BashOperator(
    #     task_id='cleanup_staging',
    #     bash_command="""
    #         docker exec postgres psql -U ${POSTGRES_USER:-postgres} -d ${POSTGRES_DB:-nyc_taxi} -c "
    #             DELETE FROM yellow_trips_staging 
    #             WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = {{ execution_date.year }}
    #               AND EXTRACT(MONTH FROM tpep_pickup_datetime) = {{ execution_date.month }};
    #         "
    #     """,
    # )
    
    # ========== DÉFINITION DES DÉPENDANCES ==========
    wait_for_ex02 >> log_params >> check_staging >> load_dimensions >> load_facts >> verify_dw
