import pandas as pd

from src.validation import (
    validate_schema,
    validate_non_negative,
    validate_all,
)


def test_validate_schema_training_ok():
    df = pd.DataFrame({
        "VendorID": [1],
        "tpep_pickup_datetime": ["2023-01-01 10:00:00"],
        "tpep_dropoff_datetime": ["2023-01-01 10:10:00"],
        "passenger_count": [1],
        "trip_distance": [2.5],
        "RatecodeID": [1],
        "store_and_fwd_flag": ["N"],
        "PULocationID": [100],
        "DOLocationID": [200],
        "payment_type": [1],
        "total_amount": [15.0],
    })

    res = validate_schema(df, require_target=True)
    assert res.is_valid is True


def test_validate_schema_inference_without_target():
    df = pd.DataFrame({
        "VendorID": [1],
        "tpep_pickup_datetime": ["2023-01-01 10:00:00"],
        "tpep_dropoff_datetime": ["2023-01-01 10:10:00"],
        "passenger_count": [1],
        "trip_distance": [2.5],
        "RatecodeID": [1],
        "store_and_fwd_flag": ["N"],
        "PULocationID": [100],
        "DOLocationID": [200],
        "payment_type": [1],
    })

    res = validate_schema(df, require_target=False)
    assert res.is_valid is True


def test_validate_non_negative_fails():
    df = pd.DataFrame({
        "passenger_count": [-1],
        "trip_distance": [2.5],
        "total_amount": [10.0],
    })

    res = validate_non_negative(df)
    assert res.is_valid is False
    assert len(res.errors) > 0


def test_validate_all_fails_on_missing_column():
    df = pd.DataFrame({
        "trip_distance": [2.5],
        "total_amount": [10.0],
    })

    res = validate_all(df, require_target=True)
    assert res.is_valid is False
