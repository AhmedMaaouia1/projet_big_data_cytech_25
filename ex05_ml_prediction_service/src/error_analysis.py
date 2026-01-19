"""
Error analysis module for ML model evaluation.

Provides detailed error analysis including:
- Residual statistics
- Error analysis by price bucket
- Top errors identification
- Business justifications for high errors
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# Price buckets for analysis (in dollars)
PRICE_BUCKETS = [
    ("low", 0, 10),
    ("medium", 10, 30),
    ("high", 30, 60),
    ("very_high", 60, float("inf")),
]


@dataclass(frozen=True)
class ErrorSummary:
    """Global error summary statistics."""

    count: int
    mean_error: float
    std_error: float
    min_error: float
    max_error: float
    median_error: float
    mean_abs_error: float
    p25_error: float
    p75_error: float
    p95_error: float
    p99_error: float
    pct_underestimate: float
    pct_overestimate: float


@dataclass(frozen=True)
class BucketErrorStats:
    """Error statistics for a price bucket."""

    bucket_name: str
    min_price: float
    max_price: float
    count: int
    pct_of_total: float
    mean_error: float
    mean_abs_error: float
    rmse: float
    median_error: float


@dataclass(frozen=True)
class TopError:
    """Details of a high-error prediction."""

    total_amount: float
    prediction: float
    error: float
    abs_error: float
    trip_distance: float
    trip_duration_min: float
    passenger_count: int
    pickup_hour: int
    payment_type: int
    pu_location_id: int
    do_location_id: int
    likely_cause: str


def _assign_price_bucket(price: float) -> str:
    """Assign a price to a bucket.

    Parameters
    ----------
    price : float
        The total amount in dollars.

    Returns
    -------
    str
        Bucket name (low, medium, high, very_high).
    """
    for name, low, high in PRICE_BUCKETS:
        if low <= price < high:
            return name
    return "unknown"


def _infer_likely_cause(row: dict) -> str:
    """Infer the likely cause of a high prediction error.

    Business justification based on trip characteristics.

    Parameters
    ----------
    row : dict
        Row data with trip features and error.

    Returns
    -------
    str
        Likely cause description.
    """
    causes = []

    # Long distance but low amount -> possible data quality issue
    if row.get("trip_distance", 0) > 20 and row.get("total_amount", 0) < 20:
        causes.append("long_distance_low_fare_anomaly")

    # Very short trip with high amount -> surge pricing or tips
    if row.get("trip_distance", 0) < 1 and row.get("total_amount", 0) > 50:
        causes.append("short_trip_high_fare_tips_or_surge")

    # Very long duration -> traffic or waiting time
    if row.get("trip_duration_min", 0) > 60:
        causes.append("extended_duration_traffic_or_wait")

    # Night hours (midnight to 5am) -> surge pricing
    hour = row.get("pickup_hour", 12)
    if 0 <= hour <= 5:
        causes.append("night_surge_pricing")

    # Airport locations (JFK=132, LGA=138) -> flat rates
    airport_ids = {132, 138, 1}  # JFK, LGA, Newark
    if row.get("pu_location_id") in airport_ids or row.get("do_location_id") in airport_ids:
        causes.append("airport_flat_rate")

    # Cash payment -> possible tip not recorded
    if row.get("payment_type") == 2:
        causes.append("cash_payment_tip_not_recorded")

    # High passenger count -> possible group fare
    if row.get("passenger_count", 1) >= 4:
        causes.append("group_ride_fare_variation")

    # Negotiated fare (ratecode 5)
    if row.get("ratecode_id") == 5:
        causes.append("negotiated_fare")

    if not causes:
        # Default: inherent model uncertainty
        if abs(row.get("error", 0)) > 20:
            causes.append("extreme_outlier")
        else:
            causes.append("model_uncertainty")

    return ", ".join(causes)


def compute_error_summary(predictions_df: DataFrame) -> ErrorSummary:
    """Compute global error statistics.

    Parameters
    ----------
    predictions_df : DataFrame
        Spark DataFrame with 'total_amount', 'prediction', and 'error' columns.

    Returns
    -------
    ErrorSummary
        Dataclass with all error statistics.
    """
    # Compute statistics
    stats = predictions_df.select(
        F.count("error").alias("count"),
        F.mean("error").alias("mean_error"),
        F.stddev("error").alias("std_error"),
        F.min("error").alias("min_error"),
        F.max("error").alias("max_error"),
        F.mean("abs_error").alias("mean_abs_error"),
        F.expr("percentile_approx(error, 0.5)").alias("median_error"),
        F.expr("percentile_approx(error, 0.25)").alias("p25_error"),
        F.expr("percentile_approx(error, 0.75)").alias("p75_error"),
        F.expr("percentile_approx(error, 0.95)").alias("p95_error"),
        F.expr("percentile_approx(error, 0.99)").alias("p99_error"),
    ).collect()[0]

    # Compute over/under estimation percentages
    total_count = stats["count"]
    under_count = predictions_df.filter(F.col("error") < 0).count()
    over_count = predictions_df.filter(F.col("error") > 0).count()

    return ErrorSummary(
        count=int(stats["count"]),
        mean_error=float(stats["mean_error"]),
        std_error=float(stats["std_error"]) if stats["std_error"] else 0.0,
        min_error=float(stats["min_error"]),
        max_error=float(stats["max_error"]),
        median_error=float(stats["median_error"]),
        mean_abs_error=float(stats["mean_abs_error"]),
        p25_error=float(stats["p25_error"]),
        p75_error=float(stats["p75_error"]),
        p95_error=float(stats["p95_error"]),
        p99_error=float(stats["p99_error"]),
        pct_underestimate=round(100.0 * under_count / total_count, 2),
        pct_overestimate=round(100.0 * over_count / total_count, 2),
    )


def compute_error_by_price_bucket(predictions_df: DataFrame) -> list[BucketErrorStats]:
    """Compute error statistics grouped by price bucket.

    Parameters
    ----------
    predictions_df : DataFrame
        Spark DataFrame with 'total_amount', 'prediction', 'error',
        'abs_error' columns.

    Returns
    -------
    list[BucketErrorStats]
        List of error stats per price bucket.
    """
    total_count = predictions_df.count()

    # Assign buckets
    bucket_expr = F.when(F.col("total_amount") < 10, "low") \
        .when(F.col("total_amount") < 30, "medium") \
        .when(F.col("total_amount") < 60, "high") \
        .otherwise("very_high")

    df_with_bucket = predictions_df.withColumn("price_bucket", bucket_expr)

    # Aggregate by bucket
    bucket_stats = df_with_bucket.groupBy("price_bucket").agg(
        F.count("*").alias("count"),
        F.mean("error").alias("mean_error"),
        F.mean("abs_error").alias("mean_abs_error"),
        F.sqrt(F.mean(F.col("error") ** 2)).alias("rmse"),
        F.expr("percentile_approx(error, 0.5)").alias("median_error"),
    ).collect()

    # Map bucket names to price ranges
    bucket_ranges = {
        "low": (0, 10),
        "medium": (10, 30),
        "high": (30, 60),
        "very_high": (60, float("inf")),
    }

    results = []
    for row in bucket_stats:
        bucket_name = row["price_bucket"]
        min_p, max_p = bucket_ranges.get(bucket_name, (0, 0))
        results.append(BucketErrorStats(
            bucket_name=bucket_name,
            min_price=min_p,
            max_price=max_p if max_p != float("inf") else 9999,
            count=int(row["count"]),
            pct_of_total=round(100.0 * row["count"] / total_count, 2),
            mean_error=round(float(row["mean_error"]), 4),
            mean_abs_error=round(float(row["mean_abs_error"]), 4),
            rmse=round(float(row["rmse"]), 4),
            median_error=round(float(row["median_error"]), 4),
        ))

    # Sort by bucket order
    order = {"low": 0, "medium": 1, "high": 2, "very_high": 3}
    results.sort(key=lambda x: order.get(x.bucket_name, 99))

    return results


def identify_top_errors(
    predictions_df: DataFrame,
    n: int = 10
) -> list[TopError]:
    """Identify the top N highest absolute errors.

    Parameters
    ----------
    predictions_df : DataFrame
        Spark DataFrame with predictions and features.
    n : int
        Number of top errors to return (default 10).

    Returns
    -------
    list[TopError]
        List of TopError dataclasses with details.
    """
    # Select relevant columns and order by absolute error
    top_rows = predictions_df.select(
        "total_amount",
        "prediction",
        "error",
        "abs_error",
        "trip_distance",
        "trip_duration_min",
        "passenger_count",
        "pickup_hour",
        "payment_type",
        F.col("PULocationID").alias("pu_location_id"),
        F.col("DOLocationID").alias("do_location_id"),
        F.col("RatecodeID").alias("ratecode_id"),
    ).orderBy(F.col("abs_error").desc()).limit(n).collect()

    results = []
    for row in top_rows:
        row_dict = row.asDict()
        likely_cause = _infer_likely_cause(row_dict)
        results.append(TopError(
            total_amount=round(float(row["total_amount"]), 2),
            prediction=round(float(row["prediction"]), 2),
            error=round(float(row["error"]), 2),
            abs_error=round(float(row["abs_error"]), 2),
            trip_distance=round(float(row["trip_distance"]), 2),
            trip_duration_min=round(float(row["trip_duration_min"]), 2),
            passenger_count=int(row["passenger_count"]) if row["passenger_count"] else 0,
            pickup_hour=int(row["pickup_hour"]),
            payment_type=int(row["payment_type"]) if row["payment_type"] else 0,
            pu_location_id=int(row["pu_location_id"]),
            do_location_id=int(row["do_location_id"]),
            likely_cause=likely_cause,
        ))

    return results


def run_error_analysis(
    predictions_df: DataFrame,
    reports_dir: str = "reports"
) -> dict[str, Any]:
    """Run complete error analysis and save reports.

    Parameters
    ----------
    predictions_df : DataFrame
        Spark DataFrame with 'total_amount' and 'prediction' columns.
    reports_dir : str
        Directory to save report files.

    Returns
    -------
    dict[str, Any]
        Dictionary with all analysis results.
    """
    os.makedirs(reports_dir, exist_ok=True)

    # Add error columns
    df_with_errors = predictions_df.withColumn(
        "error",
        F.col("total_amount") - F.col("prediction")
    ).withColumn(
        "abs_error",
        F.abs(F.col("error"))
    )

    # Cache for multiple operations
    df_with_errors.cache()

    print("\n=== Error Analysis ===")

    # 1. Global error summary
    print("[INFO] Computing global error summary...")
    error_summary = compute_error_summary(df_with_errors)
    print(f"  - Mean Error: {error_summary.mean_error:.4f}")
    print(f"  - Std Error: {error_summary.std_error:.4f}")
    print(f"  - Median Error: {error_summary.median_error:.4f}")
    print(f"  - P95 Error: {error_summary.p95_error:.4f}")
    print(f"  - Underestimate: {error_summary.pct_underestimate}%")
    print(f"  - Overestimate: {error_summary.pct_overestimate}%")

    # 2. Error by price bucket
    print("[INFO] Computing error by price bucket...")
    bucket_stats = compute_error_by_price_bucket(df_with_errors)
    for bs in bucket_stats:
        print(f"  - {bs.bucket_name}: {bs.count} trips ({bs.pct_of_total}%), "
              f"MAE={bs.mean_abs_error:.2f}, RMSE={bs.rmse:.2f}")

    # 3. Top errors
    print("[INFO] Identifying top 10 errors...")
    top_errors = identify_top_errors(df_with_errors, n=10)
    for i, te in enumerate(top_errors[:3], 1):
        print(f"  - Top {i}: actual=${te.total_amount}, "
              f"pred=${te.prediction}, error=${te.error:.2f} "
              f"({te.likely_cause})")

    # Uncache
    df_with_errors.unpersist()

    # Prepare results
    results = {
        "error_summary": asdict(error_summary),
        "error_by_price_bucket": [asdict(bs) for bs in bucket_stats],
        "top_errors": [asdict(te) for te in top_errors],
        "business_insights": _generate_business_insights(
            error_summary, bucket_stats, top_errors
        ),
    }

    # Save reports
    summary_path = os.path.join(reports_dir, "error_summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump({
            "rows_analyzed": error_summary.count,
            "summary": asdict(error_summary),
            "business_insights": results["business_insights"],
        }, f, indent=2)
    print(f"[INFO] Saved: {summary_path}")

    bucket_path = os.path.join(reports_dir, "error_by_price_bucket.json")
    with open(bucket_path, "w", encoding="utf-8") as f:
        json.dump({
            "buckets": [asdict(bs) for bs in bucket_stats],
        }, f, indent=2)
    print(f"[INFO] Saved: {bucket_path}")

    return results


def _generate_business_insights(
    summary: ErrorSummary,
    buckets: list[BucketErrorStats],
    top_errors: list[TopError]
) -> list[str]:
    """Generate business insights from error analysis.

    Parameters
    ----------
    summary : ErrorSummary
        Global error statistics.
    buckets : list[BucketErrorStats]
        Error stats by price bucket.
    top_errors : list[TopError]
        Top error cases.

    Returns
    -------
    list[str]
        List of business insight strings.
    """
    insights = []

    # Bias analysis
    if summary.pct_overestimate > 60:
        insights.append(
            f"Model tends to OVERESTIMATE fares ({summary.pct_overestimate}% of cases). "
            "This may be due to tip/surge pricing not captured in features."
        )
    elif summary.pct_underestimate > 60:
        insights.append(
            f"Model tends to UNDERESTIMATE fares ({summary.pct_underestimate}% of cases). "
            "High fares (airport, long distance) may need additional features."
        )
    else:
        insights.append(
            "Model predictions are well-balanced between over/under estimation."
        )

    # Bucket analysis
    for bucket in buckets:
        if bucket.bucket_name == "very_high" and bucket.mean_abs_error > 15:
            insights.append(
                f"High-value trips (>${bucket.min_price}) have elevated MAE "
                f"(${bucket.mean_abs_error:.2f}). Consider: airport flat rates, "
                "negotiated fares, or surge pricing."
            )
        if bucket.bucket_name == "low" and bucket.mean_abs_error > 5:
            insights.append(
                f"Low-value trips (<${bucket.max_price}) have unexpectedly high MAE "
                f"(${bucket.mean_abs_error:.2f}). Possible minimum fare effects."
            )

    # Top error causes
    cause_counts: dict[str, int] = {}
    for te in top_errors:
        for cause in te.likely_cause.split(", "):
            cause_counts[cause] = cause_counts.get(cause, 0) + 1

    if cause_counts:
        top_cause = max(cause_counts, key=cause_counts.get)
        insights.append(
            f"Most common cause of high errors: '{top_cause}' "
            f"({cause_counts[top_cause]}/10 top errors)."
        )

    return insights
