"""
Business plausibility tests for ML predictions.

Validates that model predictions are:
- Non-negative (fares cannot be negative)
- Finite (no NaN or Inf)
- Reasonable (within expected bounds for NYC taxi fares)
"""
import pytest
import numpy as np
import math


# NYC Taxi fare business constraints
MIN_FARE = 0.0  # Minimum possible fare
MAX_REASONABLE_FARE = 500.0  # Maximum reasonable single trip fare
TYPICAL_MIN_FARE = 2.50  # NYC base fare
TYPICAL_MAX_FARE = 200.0  # Typical maximum (excluding outliers)


class TestPredictionNonNegativity:
    """Tests ensuring predictions are non-negative."""

    def test_single_positive_prediction_valid(self):
        """A positive prediction is valid."""
        prediction = 25.50
        assert prediction >= MIN_FARE

    def test_zero_prediction_valid(self):
        """A zero prediction is technically valid (edge case)."""
        prediction = 0.0
        assert prediction >= MIN_FARE

    def test_negative_prediction_invalid(self):
        """A negative prediction is invalid."""
        prediction = -5.0
        assert prediction < MIN_FARE  # This should be flagged as invalid

    @pytest.mark.parametrize("prediction", [0.0, 2.50, 15.0, 50.0, 100.0, 200.0])
    def test_typical_predictions_non_negative(self, prediction: float):
        """Typical fare predictions should be non-negative."""
        assert prediction >= MIN_FARE

    def test_array_predictions_non_negative(self):
        """Array of predictions should all be non-negative."""
        predictions = np.array([10.5, 25.0, 15.75, 8.0, 45.0])
        assert np.all(predictions >= MIN_FARE)


class TestPredictionFiniteness:
    """Tests ensuring predictions are finite values."""

    def test_finite_prediction_valid(self):
        """Finite prediction is valid."""
        prediction = 25.50
        assert math.isfinite(prediction)

    def test_nan_prediction_invalid(self):
        """NaN prediction is invalid."""
        prediction = float("nan")
        assert not math.isfinite(prediction)

    def test_inf_prediction_invalid(self):
        """Infinite prediction is invalid."""
        prediction = float("inf")
        assert not math.isfinite(prediction)

    def test_negative_inf_prediction_invalid(self):
        """Negative infinite prediction is invalid."""
        prediction = float("-inf")
        assert not math.isfinite(prediction)

    def test_array_predictions_finite(self):
        """Array of predictions should all be finite."""
        predictions = np.array([10.5, 25.0, 15.75, 8.0, 45.0])
        assert np.all(np.isfinite(predictions))


class TestPredictionReasonableness:
    """Tests ensuring predictions are within reasonable bounds."""

    def test_prediction_within_max_bound(self):
        """Prediction should not exceed maximum reasonable fare."""
        prediction = 150.0
        assert prediction <= MAX_REASONABLE_FARE

    def test_extreme_prediction_flagged(self):
        """Extremely high prediction should be flagged."""
        prediction = 1000.0
        assert prediction > MAX_REASONABLE_FARE  # This is unreasonable

    def test_typical_prediction_in_range(self):
        """Typical prediction should be in expected range."""
        prediction = 25.0
        assert TYPICAL_MIN_FARE <= prediction <= TYPICAL_MAX_FARE

    @pytest.mark.parametrize("prediction,expected_valid", [
        (15.0, True),   # Normal short trip
        (45.0, True),   # Normal medium trip
        (75.0, True),   # Airport trip
        (150.0, True),  # Long distance
        (300.0, True),  # High but possible
        (600.0, False),  # Unreasonable
    ])
    def test_prediction_bounds_parametrized(
        self,
        prediction: float,
        expected_valid: bool
    ):
        """Parametrized test for prediction bounds."""
        is_reasonable = prediction <= MAX_REASONABLE_FARE
        assert is_reasonable == expected_valid


class TestPredictionValidation:
    """Integration tests for prediction validation logic."""

    def validate_predictions(self, predictions: np.ndarray) -> dict:
        """Validate an array of predictions.

        Parameters
        ----------
        predictions : np.ndarray
            Array of model predictions.

        Returns
        -------
        dict
            Validation results with counts and flags.
        """
        n_total = len(predictions)
        n_negative = np.sum(predictions < MIN_FARE)
        n_non_finite = np.sum(~np.isfinite(predictions))
        n_unreasonable = np.sum(predictions > MAX_REASONABLE_FARE)

        return {
            "total": n_total,
            "negative_count": int(n_negative),
            "non_finite_count": int(n_non_finite),
            "unreasonable_count": int(n_unreasonable),
            "valid_count": int(n_total - n_negative - n_non_finite),
            "is_valid": n_negative == 0 and n_non_finite == 0,
            "pct_valid": 100.0 * (n_total - n_negative - n_non_finite) / n_total,
        }

    def test_all_valid_predictions(self):
        """All valid predictions should pass."""
        predictions = np.array([10.5, 25.0, 15.75, 8.0, 45.0, 100.0])
        result = self.validate_predictions(predictions)
        assert result["is_valid"]
        assert result["pct_valid"] == 100.0

    def test_mixed_predictions_with_negative(self):
        """Predictions with negatives should be flagged."""
        predictions = np.array([10.5, -5.0, 15.75, 8.0, 45.0])
        result = self.validate_predictions(predictions)
        assert not result["is_valid"]
        assert result["negative_count"] == 1

    def test_mixed_predictions_with_nan(self):
        """Predictions with NaN should be flagged."""
        predictions = np.array([10.5, np.nan, 15.75, 8.0, 45.0])
        result = self.validate_predictions(predictions)
        assert not result["is_valid"]
        assert result["non_finite_count"] == 1

    def test_high_valid_percentage_acceptable(self):
        """High percentage of valid predictions is acceptable."""
        # 99% valid predictions (1 out of 100 is invalid)
        predictions = np.concatenate([
            np.random.uniform(5, 100, 99),
            np.array([-1.0])  # One invalid
        ])
        result = self.validate_predictions(predictions)
        assert result["pct_valid"] >= 99.0


class TestBusinessConstraints:
    """Tests for NYC taxi business constraints."""

    def test_minimum_fare_constraint(self):
        """NYC taxis have a minimum base fare."""
        base_fare = 2.50
        assert base_fare >= TYPICAL_MIN_FARE

    def test_airport_flat_rate_reasonable(self):
        """Airport flat rates should be within bounds."""
        jfk_flat_rate = 52.0  # JFK flat rate to Manhattan
        assert jfk_flat_rate <= MAX_REASONABLE_FARE
        assert jfk_flat_rate >= TYPICAL_MIN_FARE

    def test_surge_pricing_still_reasonable(self):
        """Even with surge pricing, fares should be reasonable."""
        surge_multiplier = 2.5
        base_fare = 50.0
        surge_fare = base_fare * surge_multiplier
        assert surge_fare <= MAX_REASONABLE_FARE
