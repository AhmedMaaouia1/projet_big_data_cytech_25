"""
============================================================================
DAG EX01 - Data Retrieval (Téléchargement et Upload vers MinIO)
============================================================================
Ce DAG orchestre le téléchargement mensuel des données NYC Yellow Taxi
depuis le site TLC et leur upload vers MinIO (Raw Zone).

Fonctionnement:
- Télécharge le fichier Parquet du mois M
- Stocke localement dans /opt/data/raw/yellow/YYYY/MM/
- Upload vers s3a://nyc-raw/yellow/YYYY/MM/

Idempotence:
- Le job EX01 vérifie si le fichier existe déjà (skip si présent)
- Mode overwrite sur MinIO

Paramètres:
- {{ ds }}: Date d'exécution Airflow (format YYYY-MM-DD)
- On extrait l'année et le mois depuis cette date

Auteur: Pipeline NYC Taxi - CY Tech Big Data
============================================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
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
    'execution_timeout': timedelta(hours=1),
}

dag = DAG(
    dag_id='ex01_data_retrieval',
    default_args=default_args,
    description='EX01 - Téléchargement NYC Taxi depuis TLC + Upload MinIO',
    schedule_interval='@monthly',
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2024, 12, 31),  # Limite pour le projet
    catchup=True,  # Permet le backfill
    max_active_runs=1,  # Un seul run à la fois
    tags=['ex01', 'ingestion', 'minio', 'raw'],
    doc_md=__doc__,
)


# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

def check_data_availability(**context):
    """
    Vérifie si les données du mois sont disponibles sur le site TLC.
    Les données sont généralement publiées avec 2 mois de retard.
    
    Returns:
        bool: True si on peut continuer, False pour skip
    """
    import urllib.request
    import urllib.error
    
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
    
    print(f"🔍 Vérification disponibilité: {url}")
    
    try:
        req = urllib.request.Request(url, method='HEAD')
        with urllib.request.urlopen(req, timeout=30) as response:
            if response.status == 200:
                print(f"✅ Données disponibles pour {year}-{month:02d}")
                return True
    except urllib.error.HTTPError as e:
        print(f"⚠️ Données non disponibles ({e.code}): {url}")
        return False
    except Exception as e:
        print(f"❌ Erreur lors de la vérification: {e}")
        return False
    
    return False


def log_execution_params(**context):
    """Log les paramètres d'exécution pour le debugging."""
    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    
    print("=" * 60)
    print("📊 PARAMÈTRES D'EXÉCUTION EX01")
    print("=" * 60)
    print(f"   Execution Date : {execution_date}")
    print(f"   Année          : {year}")
    print(f"   Mois           : {month:02d}")
    print(f"   Source URL     : https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month:02d}.parquet")
    print(f"   Destination    : s3a://nyc-raw/yellow/{year}/{month:02d}/")
    print("=" * 60)


# ============================================================================
# DÉFINITION DES TÂCHES
# ============================================================================

with dag:
    
    # Task 1: Log des paramètres
    log_params = PythonOperator(
        task_id='log_execution_params',
        python_callable=log_execution_params,
        provide_context=True,
    )
    
    # Task 2: Vérification disponibilité des données
    check_data = ShortCircuitOperator(
        task_id='check_data_availability',
        python_callable=check_data_availability,
        provide_context=True,
    )
    
    # Task 3: Exécution du job Spark EX01
    # Le job EX01 gère lui-même l'idempotence (skip si fichier existe)
    run_ex01 = BashOperator(
        task_id='spark_submit_ex01',
        bash_command="""
            docker exec spark-master spark-submit \
                --class Ex01DataRetrieval \
                --master spark://spark-master:7077 \
                --deploy-mode client \
                --driver-memory 2g \
                --executor-memory 2g \
                /opt/workdir/ex01_data_retrieval/target/scala-2.12/ex01-data-retrieval_2.12-0.1.0.jar \
                --year {{ execution_date.year }} \
                --month {{ execution_date.strftime('%m') }}
        """,
        retries=3,
        retry_delay=timedelta(minutes=2),
    )
    
    # Task 4: Vérification post-exécution
    verify_output = BashOperator(
        task_id='verify_minio_output',
        bash_command="""
            echo "🔍 Vérification du fichier dans MinIO..."
            docker exec minio-mc mc ls local/nyc-raw/yellow/{{ execution_date.year }}/{{ execution_date.strftime('%m') }}/ || {
                echo "❌ Fichier non trouvé dans MinIO"
                exit 1
            }
            echo "✅ Fichier présent dans MinIO"
        """,
    )
    
    # ========== DÉFINITION DES DÉPENDANCES ==========
    log_params >> check_data >> run_ex01 >> verify_output
