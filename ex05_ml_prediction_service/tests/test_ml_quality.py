"""
Model quality tests for ML pipeline.

Tests that validate minimum quality thresholds for the model:
- RMSE must be < 10 (project requirement)
- R² must be positive
- MAE must be reasonable

These tests can read metrics from training artifacts or use simulated values.
"""
import json
import os
import pytest
from pathlib import Path


# Project quality thresholds
MAX_RMSE_THRESHOLD = 10.0  # Project requirement: RMSE < 10
MIN_R2_THRESHOLD = 0.0  # R² must be positive (better than mean baseline)
MAX_MAE_THRESHOLD = 15.0  # Reasonable MAE for taxi fares
MIN_ACCEPTABLE_R2 = 0.5  # Good model should have R² > 0.5


# Path to training metrics (relative to project root)
METRICS_FILE = "reports/train_metrics.json"


def load_training_metrics() -> dict | None:
    """Load training metrics from the reports directory.

    Returns
    -------
    dict or None
        Metrics dictionary if file exists, None otherwise.
    """
    # Try different possible paths
    possible_paths = [
        Path(METRICS_FILE),
        Path("ex05_ml_prediction_service") / METRICS_FILE,
        Path(__file__).parent.parent / METRICS_FILE,
    ]

    for path in possible_paths:
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)

    return None


class TestRMSEQuality:
    """Tests for RMSE quality threshold."""

    def test_rmse_below_threshold_passes(self):
        """RMSE below threshold should pass."""
        rmse = 5.0
        assert rmse < MAX_RMSE_THRESHOLD, (
            f"RMSE {rmse} exceeds threshold {MAX_RMSE_THRESHOLD}"
        )

    def test_rmse_at_threshold_fails(self):
        """RMSE at or above threshold should fail."""
        rmse = 10.0
        assert rmse >= MAX_RMSE_THRESHOLD

    def test_rmse_above_threshold_fails(self):
        """RMSE above threshold should fail."""
        rmse = 15.0
        assert rmse >= MAX_RMSE_THRESHOLD

    @pytest.mark.parametrize("rmse,should_pass", [
        (3.0, True),
        (5.0, True),
        (7.5, True),
        (9.9, True),
        (10.0, False),
        (12.0, False),
        (20.0, False),
    ])
    def test_rmse_threshold_parametrized(self, rmse: float, should_pass: bool):
        """Parametrized test for RMSE threshold."""
        passes = rmse < MAX_RMSE_THRESHOLD
        assert passes == should_pass

    def test_actual_model_rmse_if_available(self):
        """Test actual model RMSE from training metrics."""
        metrics = load_training_metrics()
        if metrics is None:
            pytest.skip("Training metrics not available")

        rmse = metrics.get("metrics", {}).get("rmse")
        if rmse is None:
            pytest.skip("RMSE not found in metrics")

        assert rmse < MAX_RMSE_THRESHOLD, (
            f"Model RMSE {rmse:.4f} exceeds threshold {MAX_RMSE_THRESHOLD}"
        )


class TestR2Quality:
    """Tests for R² coefficient of determination."""

    def test_positive_r2_passes(self):
        """Positive R² should pass."""
        r2 = 0.85
        assert r2 > MIN_R2_THRESHOLD, (
            f"R² {r2} is not positive"
        )

    def test_negative_r2_fails(self):
        """Negative R² (worse than mean) should fail."""
        r2 = -0.5
        assert r2 <= MIN_R2_THRESHOLD

    def test_good_r2_threshold(self):
        """Good model should have R² above acceptable threshold."""
        r2 = 0.75
        assert r2 > MIN_ACCEPTABLE_R2, (
            f"R² {r2} is below acceptable threshold {MIN_ACCEPTABLE_R2}"
        )

    @pytest.mark.parametrize("r2,is_acceptable", [
        (0.95, True),
        (0.85, True),
        (0.60, True),
        (0.50, False),  # At threshold, not above
        (0.30, False),
        (-0.10, False),
    ])
    def test_r2_acceptability_parametrized(self, r2: float, is_acceptable: bool):
        """Parametrized test for R² acceptability."""
        is_good = r2 > MIN_ACCEPTABLE_R2
        assert is_good == is_acceptable

    def test_actual_model_r2_if_available(self):
        """Test actual model R² from training metrics."""
        metrics = load_training_metrics()
        if metrics is None:
            pytest.skip("Training metrics not available")

        r2 = metrics.get("metrics", {}).get("r2")
        if r2 is None:
            pytest.skip("R² not found in metrics")

        assert r2 > MIN_R2_THRESHOLD, (
            f"Model R² {r2:.4f} is not positive"
        )


class TestMAEQuality:
    """Tests for Mean Absolute Error quality."""

    def test_reasonable_mae_passes(self):
        """Reasonable MAE should pass."""
        mae = 5.0
        assert mae < MAX_MAE_THRESHOLD, (
            f"MAE {mae} exceeds threshold {MAX_MAE_THRESHOLD}"
        )

    def test_high_mae_fails(self):
        """High MAE should fail."""
        mae = 20.0
        assert mae >= MAX_MAE_THRESHOLD

    def test_actual_model_mae_if_available(self):
        """Test actual model MAE from training metrics."""
        metrics = load_training_metrics()
        if metrics is None:
            pytest.skip("Training metrics not available")

        mae = metrics.get("metrics", {}).get("mae")
        if mae is None:
            pytest.skip("MAE not found in metrics")

        assert mae < MAX_MAE_THRESHOLD, (
            f"Model MAE {mae:.4f} exceeds threshold {MAX_MAE_THRESHOLD}"
        )


class TestModelQualityIntegration:
    """Integration tests for overall model quality."""

    def test_all_metrics_from_artifact(self):
        """Test all metrics from training artifact."""
        metrics = load_training_metrics()
        if metrics is None:
            pytest.skip("Training metrics not available")

        model_metrics = metrics.get("metrics", {})

        rmse = model_metrics.get("rmse")
        mae = model_metrics.get("mae")
        r2 = model_metrics.get("r2")

        # Check all metrics are present
        assert rmse is not None, "RMSE not found in metrics"
        assert mae is not None, "MAE not found in metrics"
        assert r2 is not None, "R² not found in metrics"

        # Check all thresholds
        assert rmse < MAX_RMSE_THRESHOLD, f"RMSE {rmse} >= {MAX_RMSE_THRESHOLD}"
        assert r2 > MIN_R2_THRESHOLD, f"R² {r2} <= {MIN_R2_THRESHOLD}"
        assert mae < MAX_MAE_THRESHOLD, f"MAE {mae} >= {MAX_MAE_THRESHOLD}"

    def test_metrics_consistency(self):
        """Test that metrics are consistent (MAE <= RMSE)."""
        metrics = load_training_metrics()
        if metrics is None:
            pytest.skip("Training metrics not available")

        model_metrics = metrics.get("metrics", {})
        rmse = model_metrics.get("rmse")
        mae = model_metrics.get("mae")

        if rmse is None or mae is None:
            pytest.skip("Metrics not available")

        # MAE should always be <= RMSE (mathematical property)
        assert mae <= rmse, (
            f"MAE ({mae}) should be <= RMSE ({rmse})"
        )


class TestQualityThresholdConfiguration:
    """Tests for quality threshold configuration."""

    def test_rmse_threshold_is_reasonable(self):
        """RMSE threshold should be reasonable for taxi fares."""
        # $10 error on average is acceptable for taxi prediction
        assert MAX_RMSE_THRESHOLD == 10.0

    def test_r2_threshold_is_minimal(self):
        """R² threshold should be minimal (better than mean)."""
        assert MIN_R2_THRESHOLD == 0.0

    def test_thresholds_are_consistent(self):
        """Quality thresholds should be consistent."""
        # More stringent R² threshold should be higher than minimal
        assert MIN_ACCEPTABLE_R2 > MIN_R2_THRESHOLD
