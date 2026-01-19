"""
Model Registry module for ML model versioning and promotion.

Implements a simple model registry with:
- current_model: production model
- candidate_model: newly trained model awaiting evaluation

Promotion rule: if at least 2/3 metrics improve -> promote candidate to current
"""
from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Optional, Dict, Any, List
from pathlib import Path


@dataclass
class ModelMetrics:
    """Model evaluation metrics."""
    rmse: float
    mae: float
    r2: float


@dataclass
class ModelMetadata:
    """Metadata for a registered model."""
    model_id: str
    train_months: List[str]  # e.g., ["2023/03", "2023/04", "2023/05"]
    test_month: str          # e.g., "2023/06"
    metrics: ModelMetrics
    created_at: str          # ISO format
    model_path: str          # Relative path to model directory
    train_rows: int = 0
    test_rows: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "model_id": self.model_id,
            "train_months": self.train_months,
            "test_month": self.test_month,
            "metrics": asdict(self.metrics),
            "created_at": self.created_at,
            "model_path": self.model_path,
            "train_rows": self.train_rows,
            "test_rows": self.test_rows,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ModelMetadata":
        """Create from dictionary."""
        return cls(
            model_id=data["model_id"],
            train_months=data["train_months"],
            test_month=data["test_month"],
            metrics=ModelMetrics(**data["metrics"]),
            created_at=data["created_at"],
            model_path=data["model_path"],
            train_rows=data.get("train_rows", 0),
            test_rows=data.get("test_rows", 0),
        )


@dataclass
class RegistryState:
    """State of the model registry."""
    current_model: Optional[ModelMetadata] = None
    candidate_model: Optional[ModelMetadata] = None
    last_updated: str = ""
    promotion_history: List[Dict[str, Any]] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "current_model": self.current_model.to_dict() if self.current_model else None,
            "candidate_model": self.candidate_model.to_dict() if self.candidate_model else None,
            "last_updated": self.last_updated,
            "promotion_history": self.promotion_history,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RegistryState":
        """Create from dictionary."""
        return cls(
            current_model=ModelMetadata.from_dict(data["current_model"]) if data.get("current_model") else None,
            candidate_model=ModelMetadata.from_dict(data["candidate_model"]) if data.get("candidate_model") else None,
            last_updated=data.get("last_updated", ""),
            promotion_history=data.get("promotion_history", []),
        )


@dataclass
class PromotionDecision:
    """Result of model comparison and promotion decision."""
    should_promote: bool
    metrics_improved: Dict[str, bool]  # {"rmse": True, "mae": False, "r2": True}
    improvement_count: int
    reason: str
    candidate_metrics: ModelMetrics
    current_metrics: Optional[ModelMetrics]


class ModelRegistry:
    """
    Simple model registry for managing current and candidate models.
    
    Structure:
        registry_path/
            model_registry.json
            current/
                metadata/
                stages/
            candidate/
                metadata/
                stages/
    """
    
    REGISTRY_FILE = "model_registry.json"
    CURRENT_DIR = "current"
    CANDIDATE_DIR = "candidate"
    
    def __init__(self, registry_path: str):
        """
        Initialize the model registry.
        
        Parameters
        ----------
        registry_path : str
            Path to the registry directory.
        """
        self.registry_path = Path(registry_path)
        self.registry_file = self.registry_path / self.REGISTRY_FILE
        self.current_path = self.registry_path / self.CURRENT_DIR
        self.candidate_path = self.registry_path / self.CANDIDATE_DIR
        
        # Ensure registry directory exists
        self.registry_path.mkdir(parents=True, exist_ok=True)
        
        # Load or create registry state
        self.state = self._load_state()
    
    def _load_state(self) -> RegistryState:
        """Load registry state from JSON file."""
        if self.registry_file.exists():
            with open(self.registry_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                return RegistryState.from_dict(data)
        return RegistryState()
    
    def _save_state(self) -> None:
        """Save registry state to JSON file."""
        self.state.last_updated = datetime.utcnow().isoformat()
        with open(self.registry_file, "w", encoding="utf-8") as f:
            json.dump(self.state.to_dict(), f, indent=2)
    
    def get_current_model_path(self) -> Optional[str]:
        """Get path to current production model."""
        if self.state.current_model and self.current_path.exists():
            return str(self.current_path)
        return None
    
    def get_candidate_model_path(self) -> str:
        """Get path where candidate model should be saved."""
        return str(self.candidate_path)
    
    def has_current_model(self) -> bool:
        """Check if a current model exists."""
        return self.state.current_model is not None and self.current_path.exists()
    
    def register_candidate(
        self,
        train_months: List[str],
        test_month: str,
        metrics: ModelMetrics,
        train_rows: int = 0,
        test_rows: int = 0,
    ) -> ModelMetadata:
        """
        Register a new candidate model.
        
        Parameters
        ----------
        train_months : List[str]
            Months used for training (e.g., ["2023/03", "2023/04", "2023/05"])
        test_month : str
            Month used for testing (e.g., "2023/06")
        metrics : ModelMetrics
            Evaluation metrics on test set
        train_rows : int
            Number of training rows
        test_rows : int
            Number of test rows
            
        Returns
        -------
        ModelMetadata
            Metadata for the registered candidate model
        """
        model_id = f"model_{test_month.replace('/', '_')}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
        
        metadata = ModelMetadata(
            model_id=model_id,
            train_months=train_months,
            test_month=test_month,
            metrics=metrics,
            created_at=datetime.utcnow().isoformat(),
            model_path=self.CANDIDATE_DIR,
            train_rows=train_rows,
            test_rows=test_rows,
        )
        
        self.state.candidate_model = metadata
        self._save_state()
        
        return metadata
    
    def compare_models(self) -> PromotionDecision:
        """
        Compare candidate model with current model.
        
        Promotion rule: if at least 2/3 metrics improve -> promote
        - RMSE: lower is better
        - MAE: lower is better  
        - R²: higher is better
        
        Returns
        -------
        PromotionDecision
            Decision on whether to promote candidate model
        """
        if self.state.candidate_model is None:
            raise ValueError("No candidate model to compare")
        
        candidate = self.state.candidate_model.metrics
        
        # If no current model, always promote
        if self.state.current_model is None:
            return PromotionDecision(
                should_promote=True,
                metrics_improved={"rmse": True, "mae": True, "r2": True},
                improvement_count=3,
                reason="No existing current model - promoting candidate as first model",
                candidate_metrics=candidate,
                current_metrics=None,
            )
        
        current = self.state.current_model.metrics
        
        # Compare metrics (lower is better for RMSE/MAE, higher for R²)
        metrics_improved = {
            "rmse": candidate.rmse < current.rmse,
            "mae": candidate.mae < current.mae,
            "r2": candidate.r2 > current.r2,
        }
        
        improvement_count = sum(metrics_improved.values())
        should_promote = improvement_count >= 2
        
        if should_promote:
            reason = f"Candidate improves {improvement_count}/3 metrics: " + \
                     ", ".join([k for k, v in metrics_improved.items() if v])
        else:
            reason = f"Candidate only improves {improvement_count}/3 metrics - keeping current model"
        
        return PromotionDecision(
            should_promote=should_promote,
            metrics_improved=metrics_improved,
            improvement_count=improvement_count,
            reason=reason,
            candidate_metrics=candidate,
            current_metrics=current,
        )
    
    def promote_candidate(self) -> bool:
        """
        Promote candidate model to current.
        
        Replaces current model with candidate model.
        
        Returns
        -------
        bool
            True if promotion was successful
        """
        if self.state.candidate_model is None:
            raise ValueError("No candidate model to promote")
        
        if not self.candidate_path.exists():
            raise FileNotFoundError(f"Candidate model directory not found: {self.candidate_path}")
        
        # Record promotion in history
        promotion_record = {
            "timestamp": datetime.utcnow().isoformat(),
            "promoted_model": self.state.candidate_model.to_dict(),
            "replaced_model": self.state.current_model.to_dict() if self.state.current_model else None,
        }
        
        # Remove old current model
        if self.current_path.exists():
            shutil.rmtree(self.current_path)
        
        # Move candidate to current
        shutil.move(str(self.candidate_path), str(self.current_path))
        
        # Update state
        self.state.candidate_model.model_path = self.CURRENT_DIR
        self.state.current_model = self.state.candidate_model
        self.state.candidate_model = None
        self.state.promotion_history.append(promotion_record)
        
        # Keep only last 10 promotions in history
        self.state.promotion_history = self.state.promotion_history[-10:]
        
        self._save_state()
        
        return True
    
    def discard_candidate(self) -> None:
        """Discard candidate model without promoting."""
        if self.candidate_path.exists():
            shutil.rmtree(self.candidate_path)
        
        self.state.candidate_model = None
        self._save_state()
    
    def get_registry_summary(self) -> Dict[str, Any]:
        """Get a summary of the registry state for logging."""
        return {
            "registry_path": str(self.registry_path),
            "has_current_model": self.has_current_model(),
            "has_candidate_model": self.state.candidate_model is not None,
            "current_model_id": self.state.current_model.model_id if self.state.current_model else None,
            "current_test_month": self.state.current_model.test_month if self.state.current_model else None,
            "last_updated": self.state.last_updated,
            "total_promotions": len(self.state.promotion_history),
        }


def parse_month_string(month_str: str) -> tuple[int, int]:
    """
    Parse month string to (year, month) tuple.
    
    Supports formats:
    - "2023-03" 
    - "2023/03"
    
    Parameters
    ----------
    month_str : str
        Month string in YYYY-MM or YYYY/MM format
        
    Returns
    -------
    tuple[int, int]
        (year, month)
    """
    month_str = month_str.replace("-", "/")
    parts = month_str.split("/")
    return int(parts[0]), int(parts[1])


def format_month_path(year: int, month: int) -> str:
    """
    Format year/month as path string.
    
    Parameters
    ----------
    year : int
        Year (e.g., 2023)
    month : int
        Month (1-12)
        
    Returns
    -------
    str
        Formatted as "YYYY/MM"
    """
    return f"{year}/{month:02d}"


def compute_sliding_window_months(test_month: str, window_size: int = 3) -> List[str]:
    """
    Compute training months for sliding window.
    
    Given a test month M, returns months M-window_size to M-1.
    
    Parameters
    ----------
    test_month : str
        Test month in YYYY-MM or YYYY/MM format
    window_size : int
        Number of months in training window (default: 3)
        
    Returns
    -------
    List[str]
        List of training month paths (e.g., ["2023/03", "2023/04", "2023/05"])
    """
    year, month = parse_month_string(test_month)
    
    train_months = []
    for i in range(window_size, 0, -1):
        # Calculate month offset
        train_month = month - i
        train_year = year
        
        while train_month <= 0:
            train_month += 12
            train_year -= 1
        
        train_months.append(format_month_path(train_year, train_month))
    
    return train_months
