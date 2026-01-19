from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, OneHotEncoder, StringIndexer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import json
import os
import time
from datetime import datetime


from spark_session import create_spark_session
from spark_io import read_multi_months
from features import add_features
from error_analysis import run_error_analysis
from logging_config import get_logger

logger = get_logger(__name__)


def main() -> None:
    """ """
    start_time = time.time()

    spark = create_spark_session("EX05-Train")

    base_path = "s3a://nyc-interim/yellow"
    months = ["2023/01", "2023/02"]

    df = read_multi_months(spark, base_path, months)
    df = add_features(df)

    # -------------------------
    # Train / Test split
    # -------------------------
    train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)

    categorical = [
        "VendorID",
        "RatecodeID",
        "payment_type",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
    ]

    numeric = [
        "trip_distance",
        "passenger_count",
        "trip_duration_min",
        "pickup_hour",
        "pickup_dayofweek",
        "pickup_month",
    ]

    indexers = [
        StringIndexer(
            inputCol=c,
            outputCol=f"{c}_idx",
            handleInvalid="keep"
        )
        for c in categorical
    ]

    encoders = [
        OneHotEncoder(
            inputCol=f"{c}_idx",
            outputCol=f"{c}_ohe"
        )
        for c in categorical
    ]

    assembler = VectorAssembler(
        inputCols=[f"{c}_ohe" for c in categorical] + numeric,
        outputCol="features",
        handleInvalid="keep"
    )

    regressor = GBTRegressor(
        featuresCol="features",
        labelCol="total_amount",
        maxDepth=6,
        maxIter=50,
        seed=42
    )

    pipeline = Pipeline(
        stages=indexers + encoders + [assembler, regressor]
    )

    # -------------------------
    # Training
    # -------------------------
    model = pipeline.fit(train_df)

    # -------------------------
    # Evaluation
    # -------------------------
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

    rmse = rmse_evaluator.evaluate(predictions)
    mae = mae_evaluator.evaluate(predictions)
    r2 = r2_evaluator.evaluate(predictions)

    logger.info("=== Model Evaluation ===")
    logger.info(f"RMSE : {rmse:.4f}")
    logger.info(f"MAE  : {mae:.4f}")
    logger.info(f"R²   : {r2:.4f}")

    # -------------------------
    # Error Analysis
    # -------------------------
    error_analysis_results = run_error_analysis(predictions, "reports")

    # -------------------------
    # Save model
    # -------------------------
    model.write().overwrite().save("models/ex05_spark_model")

    end_time = time.time()
    duration_sec = end_time - start_time

    os.makedirs("reports", exist_ok=True)

    report = {
        "run_type": "training",
        "model": "GBTRegressor",
        "timestamp": datetime.utcnow().isoformat(),
        "data": {
            "months": months,
            "train_rows": train_df.count(),
            "test_rows": test_df.count(),
        },
        "metrics": {
            "rmse": rmse,
            "mae": mae,
            "r2": r2,
        },
        "params": {
            "maxDepth": 6,
            "maxIter": 50,
            "seed": 42,
        },
        "execution": {
            "duration_seconds": round(duration_sec, 2),
        },
        "error_analysis": {
            "summary": error_analysis_results.get("error_summary", {}),
            "business_insights": error_analysis_results.get("business_insights", []),
        },
    }

    with open("reports/train_metrics.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    spark.stop()


if __name__ == "__main__":
    main()
