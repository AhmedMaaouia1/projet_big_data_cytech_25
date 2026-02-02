from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, hour, dayofweek, month, unix_timestamp
)


def add_features(df: DataFrame) -> DataFrame:
    """Feature engineering without data leakage.

    Parameters
    ----------
    df: DataFrame :


    Returns
    -------

    """

    # Duration in minutes
    df = df.withColumn(
        "trip_duration_min",
        (
            unix_timestamp("tpep_dropoff_datetime")
            - unix_timestamp("tpep_pickup_datetime")
        ) / 60.0
    )

    # Time-based features
    df = (
        df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
          .withColumn("pickup_dayofweek", dayofweek("tpep_pickup_datetime"))
          .withColumn("pickup_month", month("tpep_pickup_datetime"))
    )

    # ✅ DATA QUALITY FILTERS (CRUCIAL)
    df = df.filter(
        col("tpep_pickup_datetime").isNotNull()
        & col("tpep_dropoff_datetime").isNotNull()
        & col("trip_duration_min").isNotNull()
        & (col("trip_duration_min") > 0)
        & (col("trip_duration_min") < 24 * 60)  # max 24h
        & col("trip_distance").isNotNull()
        & (col("trip_distance") >= 0)
        & col("total_amount").isNotNull()
        & (col("total_amount") >= 0)
    )

    # Drop rows with nulls in ML features
    df = df.dropna(subset=[
        "trip_distance",
        "passenger_count",
        "trip_duration_min",
        "pickup_hour",
        "pickup_dayofweek",
        "pickup_month",
        "VendorID",
        "RatecodeID",
        "payment_type",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "total_amount",
    ])

    # Anti-leakage: drop monetary components
    return df.drop(
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
        "airport_fee",
    )
