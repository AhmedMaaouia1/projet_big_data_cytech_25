"""
Unit tests for Airflow DAGs.

These tests verify:
1. DAGs load without errors (syntax validation)
2. DAGs have expected tasks
3. DAGs have no cycles
4. DAGs have correct schedule
5. Task dependencies are correct

Run with: pytest tests/test_dags.py -v
"""
import pytest
from datetime import datetime, timedelta
import sys
import os

# Add dags directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestDAGsIntegrity:
    """Test DAG integrity and structure."""
    
    @pytest.fixture(scope="class")
    def dagbag(self):
        """Load all DAGs from dags folder."""
        from airflow.models import DagBag
        
        dags_folder = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'dags'
        )
        return DagBag(dag_folder=dags_folder, include_examples=False)
    
    def test_dagbag_import_no_errors(self, dagbag):
        """Test that all DAGs load without import errors."""
        assert dagbag.import_errors == {}, \
            f"DAG import errors: {dagbag.import_errors}"
    
    def test_dagbag_has_dags(self, dagbag):
        """Test that at least one DAG is present."""
        assert len(dagbag.dags) > 0, "No DAGs found in dagbag"


class TestFullPipelineDAG:
    """Tests for full_nyc_taxi_pipeline DAG."""
    
    @pytest.fixture(scope="class")
    def dag(self):
        """Load the full pipeline DAG."""
        from airflow.models import DagBag
        
        dags_folder = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'dags'
        )
        dagbag = DagBag(dag_folder=dags_folder, include_examples=False)
        return dagbag.dags.get('full_nyc_taxi_pipeline')
    
    def test_dag_exists(self, dag):
        """Test that the DAG exists."""
        assert dag is not None, "full_nyc_taxi_pipeline DAG not found"
    
    def test_dag_schedule(self, dag):
        """Test that the DAG has correct monthly schedule."""
        assert dag.schedule_interval == '@monthly', \
            f"Expected @monthly schedule, got {dag.schedule_interval}"
    
    def test_dag_catchup_enabled(self, dag):
        """Test that catchup is enabled for backfill."""
        assert dag.catchup is True, "Catchup should be enabled for backfill"
    
    def test_dag_has_expected_task_count(self, dag):
        """Test that DAG has expected number of tasks."""
        # Full pipeline should have many tasks
        min_expected_tasks = 20
        task_count = len(dag.tasks)
        assert task_count >= min_expected_tasks, \
            f"Expected at least {min_expected_tasks} tasks, got {task_count}"
    
    def test_dag_has_no_cycles(self, dag):
        """Test that DAG has no cycles (is a proper DAG)."""
        # This would raise an exception if there are cycles
        from airflow.utils.dag_cycle_tester import check_cycle
        try:
            check_cycle(dag)
        except Exception as e:
            pytest.fail(f"DAG has cycles: {e}")
    
    def test_dag_has_start_task(self, dag):
        """Test that DAG has a start task."""
        task_ids = [task.task_id for task in dag.tasks]
        assert 'start_pipeline' in task_ids, "DAG should have 'start_pipeline' task"
    
    def test_dag_has_critical_tasks(self, dag):
        """Test that DAG has all critical tasks."""
        task_ids = [task.task_id for task in dag.tasks]
        
        critical_tasks = [
            'start_pipeline',
            'check_source_data_available',
            'ex01_spark_submit',
            'ex02_spark_submit',
            'ex03_load_fact_trip',
            'ex05_run_ml_pipeline',
            'pipeline_success',
        ]
        
        for task in critical_tasks:
            assert task in task_ids, f"Missing critical task: {task}"
    
    def test_dag_sla_configured(self, dag):
        """Test that SLA is configured on critical tasks."""
        sla_tasks = ['ex01_spark_submit', 'ex02_spark_submit', 'ex03_load_fact_trip', 'ex05_run_ml_pipeline']
        
        for task_id in sla_tasks:
            task = dag.get_task(task_id)
            assert task.sla is not None, f"Task {task_id} should have SLA configured"
    
    def test_dag_quality_check_exists(self, dag):
        """Test that quality check task exists."""
        task_ids = [task.task_id for task in dag.tasks]
        assert 'ex02_quality_check_retention' in task_ids, \
            "DAG should have data quality check task"
    
    def test_dag_default_args(self, dag):
        """Test that default args are correctly set."""
        assert dag.default_args['owner'] == 'nyc-taxi-pipeline'
        assert dag.default_args['retries'] >= 2
    
    def test_ex01_ex02_dependency(self, dag):
        """Test that EX02 depends on EX01 completion."""
        ex01_end = dag.get_task('ex01_complete')
        ex02_start = dag.get_task('ex02_start')
        
        assert ex02_start in ex01_end.downstream_list, \
            "EX02 should depend on EX01 completion"
    
    def test_parallel_ex03_ex05(self, dag):
        """Test that EX03 and EX05 can run in parallel after EX02."""
        ex02_end = dag.get_task('ex02_complete')
        downstream_ids = [t.task_id for t in ex02_end.downstream_list]
        
        assert 'ex03_start' in downstream_ids, "EX03 should follow EX02"
        assert 'ex05_check_can_run' in downstream_ids, "EX05 check should follow EX02"


class TestIndividualDAGs:
    """Tests for individual exercise DAGs."""
    
    @pytest.fixture(scope="class")
    def dagbag(self):
        """Load all DAGs."""
        from airflow.models import DagBag
        
        dags_folder = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'dags'
        )
        return DagBag(dag_folder=dags_folder, include_examples=False)
    
    @pytest.mark.parametrize("dag_id", [
        'ex01_data_retrieval',
        'ex02_data_ingestion',
        'ex03_dw_loading',
        'ex05_ml_pipeline',
    ])
    def test_individual_dag_exists(self, dagbag, dag_id):
        """Test that individual DAGs exist."""
        assert dag_id in dagbag.dags, f"DAG {dag_id} not found"
    
    @pytest.mark.parametrize("dag_id", [
        'ex01_data_retrieval',
        'ex02_data_ingestion',
        'ex03_dw_loading',
        'ex05_ml_pipeline',
    ])
    def test_individual_dag_has_tasks(self, dagbag, dag_id):
        """Test that individual DAGs have tasks."""
        dag = dagbag.dags.get(dag_id)
        if dag:
            assert len(dag.tasks) > 0, f"DAG {dag_id} has no tasks"
    
    @pytest.mark.parametrize("dag_id", [
        'ex01_data_retrieval',
        'ex02_data_ingestion', 
        'ex03_dw_loading',
        'ex05_ml_pipeline',
    ])
    def test_individual_dag_schedule(self, dagbag, dag_id):
        """Test that individual DAGs have monthly schedule."""
        dag = dagbag.dags.get(dag_id)
        if dag:
            assert dag.schedule_interval == '@monthly', \
                f"DAG {dag_id} should have @monthly schedule"


class TestDAGTags:
    """Tests for DAG tags (for filtering in UI)."""
    
    @pytest.fixture(scope="class")
    def dagbag(self):
        """Load all DAGs."""
        from airflow.models import DagBag
        
        dags_folder = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'dags'
        )
        return DagBag(dag_folder=dags_folder, include_examples=False)
    
    def test_full_pipeline_has_tags(self, dagbag):
        """Test that full pipeline DAG has appropriate tags."""
        dag = dagbag.dags.get('full_nyc_taxi_pipeline')
        if dag:
            assert 'production' in dag.tags, "Full pipeline should have 'production' tag"
            assert 'sla-monitored' in dag.tags, "Full pipeline should have 'sla-monitored' tag"


class TestDAGDates:
    """Tests for DAG start/end dates."""
    
    @pytest.fixture(scope="class")
    def dag(self):
        """Load the full pipeline DAG."""
        from airflow.models import DagBag
        
        dags_folder = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            'dags'
        )
        dagbag = DagBag(dag_folder=dags_folder, include_examples=False)
        return dagbag.dags.get('full_nyc_taxi_pipeline')
    
    def test_dag_start_date(self, dag):
        """Test that DAG has correct start date."""
        expected_start = datetime(2023, 1, 1)
        assert dag.start_date == expected_start, \
            f"Expected start_date {expected_start}, got {dag.start_date}"
    
    def test_dag_end_date(self, dag):
        """Test that DAG has end date set (for bounded backfill)."""
        assert dag.end_date is not None, "DAG should have end_date for bounded backfill"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
