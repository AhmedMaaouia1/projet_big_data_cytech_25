"""
Training module for ML model with sliding window support.

Supports:
- Sliding window training (e.g., train on M-3 to M-1, test on M)
- Model registry integration for versioning
- Idempotent execution
"""
from __future__ import annotations

from logging_config import get_logger

from pyspark.ml import Pipeline, PipelineModel

logger = get_logger(__name__)
from pyspark.ml.feature import VectorAssembler, OneHotEncoder, StringIndexer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import DataFrame, SparkSession
import json
import os
import time
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass

from model_registry import ModelMetrics


# Model hyperparameters
MODEL_PARAMS = {
    "maxDepth": 6,
    "maxIter": 50,
    "seed": 42,
}

# Feature definitions
CATEGORICAL_FEATURES = [
    "VendorID",
    "RatecodeID",
    "payment_type",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
]

NUMERIC_FEATURES = [
    "trip_distance",
    "passenger_count",
    "trip_duration_min",
    "pickup_hour",
    "pickup_dayofweek",
    "pickup_month",
]


@dataclass
class TrainingResult:
    """Result of model training."""
    model: PipelineModel
    metrics: ModelMetrics
    train_rows: int
    test_rows: int
    duration_seconds: float
    train_months: List[str]
    test_month: str


def build_pipeline() -> Pipeline:
    """
    Build the ML pipeline with feature engineering and GBT regressor.
    
    Returns
    -------
    Pipeline
        Spark ML Pipeline
    """
    indexers = [
        StringIndexer(
            inputCol=c,
            outputCol=f"{c}_idx",
            handleInvalid="keep"
        )
        for c in CATEGORICAL_FEATURES
    ]

    encoders = [
        OneHotEncoder(
            inputCol=f"{c}_idx",
            outputCol=f"{c}_ohe"
        )
        for c in CATEGORICAL_FEATURES
    ]

    assembler = VectorAssembler(
        inputCols=[f"{c}_ohe" for c in CATEGORICAL_FEATURES] + NUMERIC_FEATURES,
        outputCol="features",
        handleInvalid="keep"
    )

    regressor = GBTRegressor(
        featuresCol="features",
        labelCol="total_amount",
        maxDepth=MODEL_PARAMS["maxDepth"],
        maxIter=MODEL_PARAMS["maxIter"],
        seed=MODEL_PARAMS["seed"]
    )

    return Pipeline(stages=indexers + encoders + [assembler, regressor])


def evaluate_model(model: PipelineModel, test_df: DataFrame) -> ModelMetrics:
    """
    Evaluate model on test dataset.
    
    Parameters
    ----------
    model : PipelineModel
        Trained model
    test_df : DataFrame
        Test dataset
        
    Returns
    -------
    ModelMetrics
        RMSE, MAE, and R² metrics
    """
    predictions = model.transform(test_df)
    
    rmse_evaluator = RegressionEvaluator(
        labelCol="total_amount",
        predictionCol="prediction",
        metricName="rmse"
    )
    
    mae_evaluator = RegressionEvaluator(
        labelCol="total_amount",
        predictionCol="prediction",
        metricName="mae"
    )
    
    r2_evaluator = RegressionEvaluator(
        labelCol="total_amount",
        predictionCol="prediction",
        metricName="r2"
    )
    
    return ModelMetrics(
        rmse=rmse_evaluator.evaluate(predictions),
        mae=mae_evaluator.evaluate(predictions),
        r2=r2_evaluator.evaluate(predictions),
    )


def train_model(
    spark: SparkSession,
    train_df: DataFrame,
    test_df: DataFrame,
    train_months: List[str],
    test_month: str,
) -> TrainingResult:
    """
    Train a new model and evaluate on test set.
    
    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    train_df : DataFrame
        Training dataset (with features already added)
    test_df : DataFrame
        Test dataset (with features already added)
    train_months : List[str]
        Months used for training
    test_month : str
        Month used for testing
        
    Returns
    -------
    TrainingResult
        Training result with model and metrics
    """
    start_time = time.time()
    
    # Cache datasets for performance
    train_df = train_df.cache()
    test_df = test_df.cache()
    
    train_rows = train_df.count()
    test_rows = test_df.count()
    
    logger.info(f"Training on {train_rows:,} rows from months: {train_months}")
    logger.info(f"Testing on {test_rows:,} rows from month: {test_month}")
    
    # Build and fit pipeline
    pipeline = build_pipeline()
    model = pipeline.fit(train_df)
    
    # Evaluate
    metrics = evaluate_model(model, test_df)
    
    duration = time.time() - start_time
    
    logger.info("=== Model Evaluation ===")
    logger.info(f"RMSE : {metrics.rmse:.4f}")
    logger.info(f"MAE  : {metrics.mae:.4f}")
    logger.info(f"R²   : {metrics.r2:.4f}")
    logger.info(f"Duration: {duration:.1f}s")
    
    # Uncache
    train_df.unpersist()
    test_df.unpersist()
    
    return TrainingResult(
        model=model,
        metrics=metrics,
        train_rows=train_rows,
        test_rows=test_rows,
        duration_seconds=duration,
        train_months=train_months,
        test_month=test_month,
    )


def save_model(model: PipelineModel, path: str) -> None:
    """
    Save model to disk, overwriting if exists.
    
    Parameters
    ----------
    model : PipelineModel
        Trained model
    path : str
        Destination path
    """
    model.write().overwrite().save(path)
    logger.info(f"Model saved to: {path}")


def load_model(path: str) -> PipelineModel:
    """
    Load model from disk.
    
    Parameters
    ----------
    path : str
        Model path
        
    Returns
    -------
    PipelineModel
        Loaded model
    """
    return PipelineModel.load(path)


def evaluate_current_model_on_test_month(
    spark: SparkSession,
    model_path: str,
    test_df: DataFrame,
) -> ModelMetrics:
    """
    Evaluate an existing model on a new test set.
    
    Used to re-evaluate current model on same test month as candidate
    for fair comparison.
    
    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    model_path : str
        Path to existing model
    test_df : DataFrame
        Test dataset
        
    Returns
    -------
    ModelMetrics
        Evaluation metrics
    """
    model = load_model(model_path)
    return evaluate_model(model, test_df)


def generate_training_report(
    result: TrainingResult,
    promotion_decision: Optional[Dict[str, Any]] = None,
    reports_dir: str = "reports",
) -> Dict[str, Any]:
    """
    Generate training report.
    
    Parameters
    ----------
    result : TrainingResult
        Training result
    promotion_decision : Optional[Dict[str, Any]]
        Promotion decision details
    reports_dir : str
        Reports directory
        
    Returns
    -------
    Dict[str, Any]
        Report data
    """
    os.makedirs(reports_dir, exist_ok=True)
    
    report = {
        "run_type": "training",
        "model": "GBTRegressor",
        "timestamp": datetime.utcnow().isoformat(),
        "data": {
            "train_months": result.train_months,
            "test_month": result.test_month,
            "train_rows": result.train_rows,
            "test_rows": result.test_rows,
        },
        "metrics": {
            "rmse": result.metrics.rmse,
            "mae": result.metrics.mae,
            "r2": result.metrics.r2,
        },
        "params": MODEL_PARAMS,
        "execution": {
            "duration_seconds": round(result.duration_seconds, 2),
        },
    }
    
    if promotion_decision:
        report["promotion"] = promotion_decision
    
    report_path = os.path.join(reports_dir, "train_metrics.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"Report saved to: {report_path}")
    
    return report
