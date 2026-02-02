"""
Spark-based data preparation for EX05.

- Read multi-month data from MinIO
- Apply basic filters
- Feature engineering
- Sampling
- Export reduced dataset for sklearn
"""
from spark_session import create_spark_session
from pyspark.sql.functions import col, hour, dayofweek, month, unix_timestamp


def main() -> None:
    """ """
    spark = create_spark_session("EX05-ML-Prepare")

    base_path = "s3a://nyc-interim/yellow"
    months = ["2023/01", "2023/02"]
    paths = [f"{base_path}/{m}" for m in months]

    df = spark.read.parquet(*paths)

    df = df.withColumn(
        "trip_duration_min",
        (
            unix_timestamp("tpep_dropoff_datetime")
            - unix_timestamp("tpep_pickup_datetime")
        ) / 60.0
    )

    df = df.filter(
        (col("trip_duration_min") > 0)
        & (col("trip_distance") >= 0)
        & (col("total_amount") >= 0)
    )

    df = (
        df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
          .withColumn("pickup_dayofweek", dayofweek("tpep_pickup_datetime"))
          .withColumn("pickup_month", month("tpep_pickup_datetime"))
    )

    df = df.drop(
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
    )

    df.sample(fraction=0.1, seed=42) \
      .write.mode("overwrite") \
      .parquet("s3a://nyc-processed/ex05_ml_training")

    spark.stop()
