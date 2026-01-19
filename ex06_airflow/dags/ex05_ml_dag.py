"""
============================================================================
DAG EX05 - Machine Learning Pipeline (Fenêtre Glissante + Model Registry)
============================================================================
Ce DAG orchestre le pipeline ML mensuel avec stratégie de fenêtre glissante:

Stratégie:
  - Training: 3 mois précédents (M-3, M-2, M-1)
  - Test: Mois courant (M)
  - Promotion automatique si amélioration sur 2/3 métriques

Exemple pour M = Juin 2023:
  Training: Mars, Avril, Mai 2023
  Test: Juin 2023

Prérequis:
- EX02 doit avoir été exécuté pour les 4 mois (M-3 à M)
- Données présentes dans s3a://nyc-interim/yellow/

Idempotence:
- Le pipeline génère un nouveau modèle "candidate"
- La promotion ne se fait que si amélioration
- Les rapports sont horodatés

Auteur: Pipeline NYC Taxi - CY Tech Big Data
============================================================================
"""

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
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
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=3),
}

dag = DAG(
    dag_id='ex05_ml_pipeline',
    default_args=default_args,
    description='EX05 - Pipeline ML avec fenêtre glissante et Model Registry',
    schedule_interval='@monthly',
    start_date=datetime(2023, 4, 1),  # Commence en avril (besoin de 3 mois avant)
    end_date=datetime(2024, 12, 31),
    catchup=True,
    max_active_runs=1,
    tags=['ex05', 'ml', 'mlops', 'spark', 'model-registry'],
    doc_md=__doc__,
)


# ============================================================================
# FONCTIONS UTILITAIRES
# ============================================================================

def compute_sliding_window(**context):
    """
    Calcule la fenêtre glissante pour le mois d'exécution.
    
    Returns:
        dict: Contient test_month et train_months
    """
    execution_date = context['execution_date']
    
    # Mois de test = mois d'exécution
    test_month = execution_date.strftime('%Y-%m')
    
    # Mois de training = 3 mois précédents
    train_months = []
    for i in range(3, 0, -1):
        month_dt = execution_date - relativedelta(months=i)
        train_months.append(month_dt.strftime('%Y-%m'))
    
    train_months_str = ','.join(train_months)
    
    print("=" * 60)
    print("📊 FENÊTRE GLISSANTE ML")
    print("=" * 60)
    print(f"   Execution Date  : {execution_date}")
    print(f"   Test Month (M)  : {test_month}")
    print(f"   Train Months    : {train_months_str}")
    print(f"   Window          : [{train_months[0]}] → [{train_months[-1]}] → TEST [{test_month}]")
    print("=" * 60)
    
    # Push vers XCom pour les tâches suivantes
    context['ti'].xcom_push(key='test_month', value=test_month)
    context['ti'].xcom_push(key='train_months', value=train_months_str)
    
    return {
        'test_month': test_month,
        'train_months': train_months_str
    }


def check_minimum_data(**context):
    """
    Vérifie qu'on a au moins le mois de test disponible.
    Le pipeline ML utilise --skip-missing pour les mois de training manquants.
    """
    execution_date = context['execution_date']
    test_month = execution_date.strftime('%Y-%m')
    test_year, test_mm = test_month.split('-')
    
    print(f"🔍 Vérification données pour le mois de test: {test_month}")
    print(f"   Le pipeline ML tolère des mois de training manquants (--skip-missing)")
    print(f"   Mais le mois de TEST est OBLIGATOIRE")
    
    return True  # Validation détaillée faite par le script ML lui-même


# ============================================================================
# DÉFINITION DES TÂCHES
# ============================================================================

with dag:
    
    # Task 1: Attente de la fin de EX02 pour le mois de test
    # Note: On attend seulement le mois courant, pas les mois de training
    wait_for_ex02 = ExternalTaskSensor(
        task_id='wait_for_ex02_test_month',
        external_dag_id='ex02_data_ingestion',
        external_task_id='verify_minio_interim',
        mode='poke',
        timeout=3600,
        poke_interval=60,
        allowed_states=['success'],
    )
    
    # Task 2: Calcul de la fenêtre glissante
    compute_window = PythonOperator(
        task_id='compute_sliding_window',
        python_callable=compute_sliding_window,
        provide_context=True,
    )
    
    # Task 3: Vérification minimale des données
    check_data = PythonOperator(
        task_id='check_minimum_data',
        python_callable=check_minimum_data,
        provide_context=True,
    )
    
    # Task 4: Vérification données MinIO pour le mois de test
    verify_test_data = BashOperator(
        task_id='verify_test_month_data',
        bash_command="""
            TEST_MONTH="{{ ti.xcom_pull(task_ids='compute_sliding_window', key='test_month') }}"
            YEAR=$(echo $TEST_MONTH | cut -d'-' -f1)
            MONTH=$(echo $TEST_MONTH | cut -d'-' -f2)
            
            echo "🔍 Vérification données test dans MinIO..."
            echo "   Mois de test: $TEST_MONTH"
            
            docker exec minio-mc mc ls local/nyc-interim/yellow/$YEAR/$MONTH/ || {
                echo "❌ Données du mois de test non trouvées!"
                echo "   Chemin attendu: nyc-interim/yellow/$YEAR/$MONTH/"
                exit 1
            }
            echo "✅ Données du mois de test présentes"
        """,
    )
    
    # Task 5: Exécution du pipeline ML
    # Utilise spark-submit depuis le conteneur Spark
    run_ml_pipeline = BashOperator(
        task_id='run_ml_pipeline',
        bash_command="""
            TEST_MONTH="{{ ti.xcom_pull(task_ids='compute_sliding_window', key='test_month') }}"
            TRAIN_MONTHS="{{ ti.xcom_pull(task_ids='compute_sliding_window', key='train_months') }}"
            
            echo "=========================================="
            echo "🚀 EXÉCUTION PIPELINE ML"
            echo "=========================================="
            echo "   Test Month   : $TEST_MONTH"
            echo "   Train Months : $TRAIN_MONTHS"
            echo "=========================================="
            
            # Exécution via spark-submit dans le conteneur Spark Master
            docker exec spark-master spark-submit \
                --master spark://spark-master:7077 \
                --deploy-mode client \
                --driver-memory 4g \
                --executor-memory 4g \
                --conf spark.driver.extraJavaOptions="-Dlog4j2.configurationFile=/opt/workdir/ex05_ml_prediction_service/conf/log4j2.properties" \
                /opt/workdir/ex05_ml_prediction_service/src/ml_pipeline.py \
                --test-month "$TEST_MONTH" \
                --train-months "$TRAIN_MONTHS" \
                --model-registry-path /opt/workdir/ex05_ml_prediction_service/models/registry \
                --data-base-path s3a://nyc-interim/yellow \
                --reports-dir /opt/workdir/ex05_ml_prediction_service/reports \
                --skip-missing
            
            EXIT_CODE=$?
            
            if [ $EXIT_CODE -eq 0 ]; then
                echo "✅ Pipeline ML terminé avec succès"
            else
                echo "❌ Pipeline ML échoué avec code: $EXIT_CODE"
                exit $EXIT_CODE
            fi
        """,
        retries=2,
        retry_delay=timedelta(minutes=5),
    )
    
    # Task 6: Vérification des rapports générés
    verify_reports = BashOperator(
        task_id='verify_ml_reports',
        bash_command="""
            echo "📊 Vérification des rapports ML..."
            
            REPORTS_DIR="/opt/workdir/ex05_ml_prediction_service/reports"
            
            docker exec spark-master bash -c "
                echo 'Rapports générés:'
                ls -la $REPORTS_DIR/
                
                if [ -f '$REPORTS_DIR/train_metrics.json' ]; then
                    echo ''
                    echo '📈 Métriques d\\'entraînement:'
                    cat $REPORTS_DIR/train_metrics.json
                fi
            "
            
            echo "✅ Rapports vérifiés"
        """,
    )
    
    # Task 7: Vérification du Model Registry
    verify_registry = BashOperator(
        task_id='verify_model_registry',
        bash_command="""
            echo "🗂️ Vérification du Model Registry..."
            
            REGISTRY_PATH="/opt/workdir/ex05_ml_prediction_service/models/registry"
            
            docker exec spark-master bash -c "
                if [ -f '$REGISTRY_PATH/model_registry.json' ]; then
                    echo 'Contenu du registry:'
                    cat $REGISTRY_PATH/model_registry.json
                else
                    echo 'Registry non encore initialisé (premier run)'
                fi
                
                echo ''
                echo 'Structure du registry:'
                ls -la $REGISTRY_PATH/ 2>/dev/null || echo 'Dossier registry non créé'
            "
            
            echo "✅ Model Registry vérifié"
        """,
    )
    
    # ========== DÉFINITION DES DÉPENDANCES ==========
    wait_for_ex02 >> compute_window >> check_data >> verify_test_data
    verify_test_data >> run_ml_pipeline >> [verify_reports, verify_registry]
