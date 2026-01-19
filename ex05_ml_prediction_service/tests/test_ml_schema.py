"""
ML Schema tests for training and inference.

Validates that required columns are present and have correct types
for the ML pipeline to function correctly.
"""
import pytest
import pandas as pd

from src.validation import REQUIRED_COLUMNS, validate_schema


# Columns required for training (with target)
TRAINING_COLUMNS = REQUIRED_COLUMNS.copy()

# Columns required for inference (without target)
INFERENCE_COLUMNS = [c for c in REQUIRED_COLUMNS if c != "total_amount"]

# Feature columns used by the model
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
]

# Derived features added during feature engineering
DERIVED_FEATURES = [
    "trip_duration_min",
    "pickup_hour",
    "pickup_dayofweek",
    "pickup_month",
]


def _create_valid_training_df() -> pd.DataFrame:
    """Create a valid training DataFrame with all required columns."""
    return pd.DataFrame({
        "VendorID": [1, 2],
        "tpep_pickup_datetime": ["2023-01-01 10:00:00", "2023-01-01 11:00:00"],
        "tpep_dropoff_datetime": ["2023-01-01 10:15:00", "2023-01-01 11:20:00"],
        "passenger_count": [1, 2],
        "trip_distance": [2.5, 5.0],
        "RatecodeID": [1, 1],
        "store_and_fwd_flag": ["N", "N"],
        "PULocationID": [100, 150],
        "DOLocationID": [200, 250],
        "payment_type": [1, 2],
        "total_amount": [15.0, 25.0],
    })


def _create_valid_inference_df() -> pd.DataFrame:
    """Create a valid inference DataFrame without target column."""
    df = _create_valid_training_df()
    return df.drop(columns=["total_amount"])


class TestTrainingSchema:
    """Tests for training data schema validation."""

    def test_all_required_columns_present(self):
        """Training data must have all required columns including target."""
        df = _create_valid_training_df()
        result = validate_schema(df, require_target=True)
        assert result.is_valid, f"Schema validation failed: {result.errors}"

    def test_missing_target_fails(self):
        """Training without target column must fail."""
        df = _create_valid_inference_df()
        result = validate_schema(df, require_target=True)
        assert not result.is_valid
        assert any("total_amount" in str(e) for e in result.errors)

    def test_missing_feature_column_fails(self):
        """Training with missing feature column must fail."""
        df = _create_valid_training_df()
        df = df.drop(columns=["trip_distance"])
        result = validate_schema(df, require_target=True)
        assert not result.is_valid
        assert any("trip_distance" in str(e) for e in result.errors)

    def test_missing_categorical_feature_fails(self):
        """Training with missing categorical feature must fail."""
        df = _create_valid_training_df()
        df = df.drop(columns=["VendorID"])
        result = validate_schema(df, require_target=True)
        assert not result.is_valid
        assert any("VendorID" in str(e) for e in result.errors)

    @pytest.mark.parametrize("col", CATEGORICAL_FEATURES)
    def test_each_categorical_feature_required(self, col: str):
        """Each categorical feature must be present."""
        df = _create_valid_training_df()
        if col in df.columns:
            df = df.drop(columns=[col])
            result = validate_schema(df, require_target=True)
            assert not result.is_valid


class TestInferenceSchema:
    """Tests for inference data schema validation."""

    def test_inference_without_target_valid(self):
        """Inference data without target column must be valid."""
        df = _create_valid_inference_df()
        result = validate_schema(df, require_target=False)
        assert result.is_valid, f"Schema validation failed: {result.errors}"

    def test_inference_with_target_also_valid(self):
        """Inference data with target column is also valid."""
        df = _create_valid_training_df()
        result = validate_schema(df, require_target=False)
        assert result.is_valid

    def test_inference_missing_pickup_datetime_fails(self):
        """Inference without pickup datetime must fail."""
        df = _create_valid_inference_df()
        df = df.drop(columns=["tpep_pickup_datetime"])
        result = validate_schema(df, require_target=False)
        assert not result.is_valid

    def test_inference_missing_location_fails(self):
        """Inference without location columns must fail."""
        df = _create_valid_inference_df()
        df = df.drop(columns=["PULocationID"])
        result = validate_schema(df, require_target=False)
        assert not result.is_valid


class TestFeatureColumns:
    """Tests verifying feature columns are correctly defined."""

    def test_all_categorical_features_in_required(self):
        """All categorical features must be in required columns."""
        for col in CATEGORICAL_FEATURES:
            assert col in REQUIRED_COLUMNS, f"Categorical feature {col} not in REQUIRED_COLUMNS"

    def test_all_numeric_features_in_required(self):
        """All numeric features must be in required columns."""
        for col in NUMERIC_FEATURES:
            assert col in REQUIRED_COLUMNS, f"Numeric feature {col} not in REQUIRED_COLUMNS"

    def test_datetime_columns_in_required(self):
        """Datetime columns must be in required columns."""
        assert "tpep_pickup_datetime" in REQUIRED_COLUMNS
        assert "tpep_dropoff_datetime" in REQUIRED_COLUMNS
