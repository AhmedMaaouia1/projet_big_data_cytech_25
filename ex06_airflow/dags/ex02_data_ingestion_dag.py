"""
============================================================================
DAG EX02 - Data Ingestion (Nettoyage + Double Branche)
============================================================================
Ce DAG orchestre l'ingestion et le nettoyage des données NYC Yellow Taxi
avec une architecture double branche:

  Branch 1: Écriture vers MinIO (nyc-interim) - Parquet pour ML
  Branch 2: Écriture vers PostgreSQL (staging) - pour Data Warehouse

Prérequis:
- EX01 doit avoir été exécuté pour le mois M (données dans nyc-raw)

Idempotence:
- Branch 1: Mode overwrite sur la partition mensuelle MinIO
- Branch 2: Mode overwrite + truncate sur staging

Paramètres:
- {{ ds }}: Date d'exécution Airflow (format YYYY-MM-DD)

Auteur: Pipeline NYC Taxi - CY Tech Big Data
============================================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago


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
    dag_id='ex02_data_ingestion',
    default_args=default_args,
    description='EX02 - Nettoyage + Écriture MinIO (interim) et PostgreSQL (staging)',
    schedule_interval='@monthly',
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2024, 12, 31),
    catchup=True,
    max_active_runs=1,
    tags=['ex02', 'ingestion', 'cleaning', 'minio', 'postgres'],
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
    print("📊 PARAMÈTRES D'EXÉCUTION EX02")
    print("=" * 60)
    print(f"   Execution Date : {execution_date}")
    print(f"   Année          : {year}")
    print(f"   Mois           : {month:02d}")
    print(f"   Source         : s3a://nyc-raw/yellow/{year}/{month:02d}/")
    print(f"   Branch 1       : s3a://nyc-interim/yellow/{year}/{month:02d}/")
    print(f"   Branch 2       : PostgreSQL staging")
    print("=" * 60)


# ============================================================================
# DÉFINITION DES TÂCHES
# ============================================================================

with dag:
    
    # Task 1: Attente de la fin de EX01 pour le même mois
    wait_for_ex01 = ExternalTaskSensor(
        task_id='wait_for_ex01_completion',
        external_dag_id='ex01_data_retrieval',
        external_task_id='verify_minio_output',
        mode='poke',
        timeout=3600,  # 1 heure max d'attente
        poke_interval=60,  # Vérifie toutes les minutes
        allowed_states=['success'],
    )
    
    # Task 2: Log des paramètres
    log_params = PythonOperator(
        task_id='log_execution_params',
        python_callable=log_execution_params,
        provide_context=True,
    )
    
    # Task 3: Vérification des données source
    check_source = BashOperator(
        task_id='check_raw_data_exists',
        bash_command="""
            echo "🔍 Vérification données source dans MinIO raw..."
            docker exec minio-mc mc ls local/nyc-raw/yellow/{{ execution_date.year }}/{{ execution_date.strftime('%m') }}/ || {
                echo "❌ Données source non trouvées dans nyc-raw"
                exit 1
            }
            echo "✅ Données source présentes"
        """,
    )
    
    # Task 4: Exécution du job Spark EX02 (double branche)
    run_ex02 = BashOperator(
        task_id='spark_submit_ex02',
        bash_command="""
            docker exec spark-master spark-submit \
                --class Ex02DataIngestion \
                --master spark://spark-master:7077 \
                --deploy-mode client \
                --driver-memory 2g \
                --executor-memory 2g \
                --jars /opt/spark/jars/postgresql-42.7.4.jar \
                /opt/workdir/ex02_data_ingestion/target/scala-2.12/ex02-data-ingestion_2.12-0.1.0.jar \
                --year {{ execution_date.year }} \
                --month {{ execution_date.strftime('%m') }} \
                --enableDw true
        """,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )
    
    # Task 5: Vérification Branch 1 (MinIO interim)
    verify_branch1 = BashOperator(
        task_id='verify_minio_interim',
        bash_command="""
            echo "🔍 Vérification Branch 1 (MinIO interim)..."
            docker exec minio-mc mc ls local/nyc-interim/yellow/{{ execution_date.year }}/{{ execution_date.strftime('%m') }}/ || {
                echo "❌ Données non trouvées dans nyc-interim"
                exit 1
            }
            echo "✅ Branch 1 OK - Données dans nyc-interim"
        """,
    )
    
    # Task 6: Vérification Branch 2 (PostgreSQL staging)
    verify_branch2 = BashOperator(
        task_id='verify_postgres_staging',
        bash_command="""
            echo "🔍 Vérification Branch 2 (PostgreSQL staging)..."
            STAGING_COUNT=$(docker exec postgres psql -U ${POSTGRES_USER:-postgres} -d ${POSTGRES_DB:-nyc_taxi} -t -c \
                "SELECT COUNT(*) FROM yellow_trips_staging WHERE EXTRACT(YEAR FROM tpep_pickup_datetime) = {{ execution_date.year }} AND EXTRACT(MONTH FROM tpep_pickup_datetime) = {{ execution_date.month }};" 2>/dev/null | tr -d ' ')
            
            if [ -z "$STAGING_COUNT" ] || [ "$STAGING_COUNT" -eq 0 ]; then
                echo "❌ Aucune donnée dans staging pour {{ execution_date.year }}-{{ execution_date.strftime('%m') }}"
                exit 1
            fi
            
            echo "✅ Branch 2 OK - $STAGING_COUNT enregistrements dans staging"
        """,
    )
    
    # ========== DÉFINITION DES DÉPENDANCES ==========
    wait_for_ex01 >> log_params >> check_source >> run_ex02
    run_ex02 >> [verify_branch1, verify_branch2]
