"""
Tests for model registry functionality.
"""
import json
import os
import shutil
import tempfile
from pathlib import Path

import pytest

from model_registry import (
    ModelMetrics,
    ModelMetadata,
    ModelRegistry,
    RegistryState,
    PromotionDecision,
    compute_sliding_window_months,
    parse_month_string,
    format_month_path,
)


class TestModelMetrics:
    """Tests for ModelMetrics dataclass."""

    def test_create_metrics(self):
        """Test creating metrics."""
        metrics = ModelMetrics(rmse=5.0, mae=3.0, r2=0.85)
        assert metrics.rmse == 5.0
        assert metrics.mae == 3.0
        assert metrics.r2 == 0.85


class TestModelMetadata:
    """Tests for ModelMetadata dataclass."""

    def test_to_dict(self):
        """Test serialization to dict."""
        metrics = ModelMetrics(rmse=5.0, mae=3.0, r2=0.85)
        metadata = ModelMetadata(
            model_id="model_2023_06",
            train_months=["2023/03", "2023/04", "2023/05"],
            test_month="2023/06",
            metrics=metrics,
            created_at="2023-06-01T00:00:00",
            model_path="candidate",
            train_rows=100000,
            test_rows=10000,
        )
        
        d = metadata.to_dict()
        assert d["model_id"] == "model_2023_06"
        assert d["train_months"] == ["2023/03", "2023/04", "2023/05"]
        assert d["metrics"]["rmse"] == 5.0

    def test_from_dict(self):
        """Test deserialization from dict."""
        data = {
            "model_id": "model_2023_06",
            "train_months": ["2023/03", "2023/04", "2023/05"],
            "test_month": "2023/06",
            "metrics": {"rmse": 5.0, "mae": 3.0, "r2": 0.85},
            "created_at": "2023-06-01T00:00:00",
            "model_path": "candidate",
            "train_rows": 100000,
            "test_rows": 10000,
        }
        
        metadata = ModelMetadata.from_dict(data)
        assert metadata.model_id == "model_2023_06"
        assert metadata.metrics.rmse == 5.0


class TestMonthParsing:
    """Tests for month parsing utilities."""

    def test_parse_month_dash(self):
        """Test parsing YYYY-MM format."""
        year, month = parse_month_string("2023-06")
        assert year == 2023
        assert month == 6

    def test_parse_month_slash(self):
        """Test parsing YYYY/MM format."""
        year, month = parse_month_string("2023/06")
        assert year == 2023
        assert month == 6

    def test_format_month_path(self):
        """Test formatting month path."""
        path = format_month_path(2023, 6)
        assert path == "2023/06"
        
        path = format_month_path(2023, 1)
        assert path == "2023/01"


class TestSlidingWindow:
    """Tests for sliding window computation."""

    def test_sliding_window_mid_year(self):
        """Test sliding window in middle of year."""
        months = compute_sliding_window_months("2023-06", window_size=3)
        assert months == ["2023/03", "2023/04", "2023/05"]

    def test_sliding_window_year_boundary(self):
        """Test sliding window crossing year boundary."""
        months = compute_sliding_window_months("2023-02", window_size=3)
        assert months == ["2022/11", "2022/12", "2023/01"]

    def test_sliding_window_january(self):
        """Test sliding window for January."""
        months = compute_sliding_window_months("2023-01", window_size=3)
        assert months == ["2022/10", "2022/11", "2022/12"]

    def test_sliding_window_custom_size(self):
        """Test sliding window with custom size."""
        months = compute_sliding_window_months("2023-06", window_size=2)
        assert months == ["2023/04", "2023/05"]


class TestModelRegistry:
    """Tests for ModelRegistry class."""

    @pytest.fixture
    def temp_registry_path(self):
        """Create temporary registry directory."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        shutil.rmtree(temp_dir)

    def test_create_registry(self, temp_registry_path):
        """Test creating a new registry."""
        registry = ModelRegistry(temp_registry_path)
        
        assert registry.registry_path.exists()
        assert not registry.has_current_model()
        assert registry.state.current_model is None

    def test_register_candidate(self, temp_registry_path):
        """Test registering a candidate model."""
        registry = ModelRegistry(temp_registry_path)
        
        metrics = ModelMetrics(rmse=5.0, mae=3.0, r2=0.85)
        metadata = registry.register_candidate(
            train_months=["2023/03", "2023/04", "2023/05"],
            test_month="2023/06",
            metrics=metrics,
            train_rows=100000,
            test_rows=10000,
        )
        
        assert metadata.test_month == "2023/06"
        assert registry.state.candidate_model is not None
        assert registry.state.candidate_model.metrics.rmse == 5.0

    def test_compare_no_current(self, temp_registry_path):
        """Test comparing when no current model exists."""
        registry = ModelRegistry(temp_registry_path)
        
        metrics = ModelMetrics(rmse=5.0, mae=3.0, r2=0.85)
        registry.register_candidate(
            train_months=["2023/03", "2023/04", "2023/05"],
            test_month="2023/06",
            metrics=metrics,
        )
        
        decision = registry.compare_models()
        
        assert decision.should_promote is True
        assert decision.improvement_count == 3
        assert "first model" in decision.reason.lower()

    def test_compare_candidate_better(self, temp_registry_path):
        """Test comparing when candidate is better."""
        registry = ModelRegistry(temp_registry_path)
        
        # Simulate current model
        current_metrics = ModelMetrics(rmse=6.0, mae=4.0, r2=0.80)
        registry.state.current_model = ModelMetadata(
            model_id="old_model",
            train_months=["2023/02", "2023/03", "2023/04"],
            test_month="2023/05",
            metrics=current_metrics,
            created_at="2023-05-01T00:00:00",
            model_path="current",
        )
        
        # Register better candidate
        candidate_metrics = ModelMetrics(rmse=5.0, mae=3.0, r2=0.85)
        registry.register_candidate(
            train_months=["2023/03", "2023/04", "2023/05"],
            test_month="2023/06",
            metrics=candidate_metrics,
        )
        
        decision = registry.compare_models()
        
        assert decision.should_promote is True
        assert decision.improvement_count == 3
        assert all(decision.metrics_improved.values())

    def test_compare_candidate_worse(self, temp_registry_path):
        """Test comparing when candidate is worse."""
        registry = ModelRegistry(temp_registry_path)
        
        # Simulate current model (better)
        current_metrics = ModelMetrics(rmse=4.0, mae=2.5, r2=0.90)
        registry.state.current_model = ModelMetadata(
            model_id="good_model",
            train_months=["2023/02", "2023/03", "2023/04"],
            test_month="2023/05",
            metrics=current_metrics,
            created_at="2023-05-01T00:00:00",
            model_path="current",
        )
        
        # Register worse candidate
        candidate_metrics = ModelMetrics(rmse=6.0, mae=4.0, r2=0.80)
        registry.register_candidate(
            train_months=["2023/03", "2023/04", "2023/05"],
            test_month="2023/06",
            metrics=candidate_metrics,
        )
        
        decision = registry.compare_models()
        
        assert decision.should_promote is False
        assert decision.improvement_count == 0
        assert not any(decision.metrics_improved.values())

    def test_compare_candidate_partial_improvement(self, temp_registry_path):
        """Test comparing when candidate improves only 1 metric."""
        registry = ModelRegistry(temp_registry_path)
        
        # Simulate current model
        current_metrics = ModelMetrics(rmse=5.0, mae=3.0, r2=0.85)
        registry.state.current_model = ModelMetadata(
            model_id="current_model",
            train_months=["2023/02", "2023/03", "2023/04"],
            test_month="2023/05",
            metrics=current_metrics,
            created_at="2023-05-01T00:00:00",
            model_path="current",
        )
        
        # Register candidate that only improves R² 
        candidate_metrics = ModelMetrics(rmse=5.5, mae=3.5, r2=0.90)
        registry.register_candidate(
            train_months=["2023/03", "2023/04", "2023/05"],
            test_month="2023/06",
            metrics=candidate_metrics,
        )
        
        decision = registry.compare_models()
        
        assert decision.should_promote is False
        assert decision.improvement_count == 1
        assert decision.metrics_improved["r2"] is True
        assert decision.metrics_improved["rmse"] is False
        assert decision.metrics_improved["mae"] is False

    def test_promotion_threshold_2_of_3(self, temp_registry_path):
        """Test that promotion requires at least 2/3 metrics improvement."""
        registry = ModelRegistry(temp_registry_path)
        
        # Simulate current model
        current_metrics = ModelMetrics(rmse=5.0, mae=3.0, r2=0.85)
        registry.state.current_model = ModelMetadata(
            model_id="current_model",
            train_months=["2023/02", "2023/03", "2023/04"],
            test_month="2023/05",
            metrics=current_metrics,
            created_at="2023-05-01T00:00:00",
            model_path="current",
        )
        
        # Register candidate that improves 2 metrics
        candidate_metrics = ModelMetrics(rmse=4.5, mae=3.2, r2=0.88)
        registry.register_candidate(
            train_months=["2023/03", "2023/04", "2023/05"],
            test_month="2023/06",
            metrics=candidate_metrics,
        )
        
        decision = registry.compare_models()
        
        assert decision.should_promote is True
        assert decision.improvement_count == 2

    def test_registry_persistence(self, temp_registry_path):
        """Test that registry state persists across instances."""
        # Create and modify registry
        registry1 = ModelRegistry(temp_registry_path)
        metrics = ModelMetrics(rmse=5.0, mae=3.0, r2=0.85)
        registry1.register_candidate(
            train_months=["2023/03", "2023/04", "2023/05"],
            test_month="2023/06",
            metrics=metrics,
        )
        
        # Load registry in new instance
        registry2 = ModelRegistry(temp_registry_path)
        
        assert registry2.state.candidate_model is not None
        assert registry2.state.candidate_model.test_month == "2023/06"

    def test_get_registry_summary(self, temp_registry_path):
        """Test getting registry summary."""
        registry = ModelRegistry(temp_registry_path)
        
        summary = registry.get_registry_summary()
        
        assert "registry_path" in summary
        assert summary["has_current_model"] is False
        assert summary["has_candidate_model"] is False
