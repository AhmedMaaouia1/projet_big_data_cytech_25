from pyspark.ml import PipelineModel

from spark_session import create_spark_session
from features import add_features
from validation import validate_schema
import json
import os
import time
from datetime import datetime


def main() -> None:
    """ """
    start_time = time.time()

    spark = create_spark_session("EX05-Predict")

    # Load trained Spark ML pipeline
    model = PipelineModel.load("models/ex05_spark_model")

    # Read inference data
    df = spark.read.parquet("s3a://nyc-interim/yellow/2023/02")

    # Feature engineering (must match training)
    df = add_features(df)

    # -------------------------
    # Input validation (sample)
    # -------------------------
    # Convert a small sample to pandas for fast validation
    pdf = df.limit(1000).toPandas()

    res = validate_schema(pdf, require_target=False)
    if not res.is_valid:
        raise ValueError(f"Invalid inference input data: {res.errors}")

    # -------------------------
    # Inference
    # -------------------------
    preds = model.transform(df)

    preds.select(
        "total_amount",
        "prediction"
    ).show(20, truncate=False)

    end_time = time.time()
    duration_sec = end_time - start_time

    os.makedirs("reports", exist_ok=True)

    report = {
        "run_type": "inference",
        "timestamp": datetime.utcnow().isoformat(),
        "data": {
            "input_path": "s3a://nyc-interim/yellow/2023/02",
            "rows_scored": preds.count(),
        },
        "validation": {
            "schema_valid": True,
            "sample_size": 1000,
        },
        "execution": {
            "duration_seconds": round(duration_sec, 2),
        },
    }

    with open("reports/predict_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    spark.stop()


if __name__ == "__main__":
    main()
