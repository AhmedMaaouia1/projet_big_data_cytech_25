"""
Configuration module for EX05.

Supports:
- Multi-month training ranges
- Sliding window ML strategy
- Model registry configuration
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class MinioConfig:
    """MinIO connection configuration."""
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str
    use_ssl: bool = False


@dataclass(frozen=True)
class DataRangeConfig:
    """Training data range configuration (legacy - kept for compatibility)."""
    start_year: int
    start_month: int
    end_year: int
    end_month: int
    prefix: str = "yellow"


@dataclass(frozen=True)
class SlidingWindowConfig:
    """Sliding window ML strategy configuration."""
    window_size: int = 3  # Number of months for training
    data_base_path: str = "s3a://nyc-interim/yellow"


@dataclass(frozen=True)
class ModelRegistryConfig:
    """Model registry configuration."""
    registry_path: str = "models/registry"
    max_history: int = 10  # Max promotions to keep in history
    promotion_threshold: int = 2  # Min metrics that must improve (out of 3)


@dataclass(frozen=True)
class PathsConfig:
    """Local paths configuration."""
    models_dir: str = "models"
    reports_dir: str = "reports"
    registry_dir: str = "models/registry"


def _get_env(name: str) -> str:
    """

    Parameters
    ----------
    name: str :


    Returns
    -------

    """
    value = os.getenv(name)
    if not value:
        raise EnvironmentError(f"Missing required environment variable: {name}")
    return value


def load_minio_config() -> MinioConfig:
    """Load MinIO config from environment variables."""
    return MinioConfig(
        endpoint_url=_get_env("MINIO_ENDPOINT"),
        access_key=_get_env("MINIO_ACCESS_KEY"),
        secret_key=_get_env("MINIO_SECRET_KEY"),
        bucket=_get_env("MINIO_BUCKET_INTERIM"),
        use_ssl=os.getenv("MINIO_USE_SSL", "false").lower() == "true",
    )


def load_data_range_config() -> DataRangeConfig:
    """Load dataset range from environment variables.

    Required:
    - DATA_START_YEAR, DATA_START_MONTH
    - DATA_END_YEAR, DATA_END_MONTH

    Parameters
    ----------

    Returns
    -------

    """
    return DataRangeConfig(
        start_year=int(_get_env("DATA_START_YEAR")),
        start_month=int(_get_env("DATA_START_MONTH")),
        end_year=int(_get_env("DATA_END_YEAR")),
        end_month=int(_get_env("DATA_END_MONTH")),
    )


def load_paths_config() -> PathsConfig:
    """Load local paths config (override optional)."""
    return PathsConfig(
        models_dir=os.getenv("MODELS_DIR", "models"),
        reports_dir=os.getenv("REPORTS_DIR", "reports"),
        registry_dir=os.getenv("MODEL_REGISTRY_PATH", "models/registry"),
    )


def load_sliding_window_config() -> SlidingWindowConfig:
    """Load sliding window configuration from environment."""
    return SlidingWindowConfig(
        window_size=int(os.getenv("ML_WINDOW_SIZE", "3")),
        data_base_path=os.getenv("DATA_BASE_PATH", "s3a://nyc-interim/yellow"),
    )


def load_model_registry_config() -> ModelRegistryConfig:
    """Load model registry configuration from environment."""
    return ModelRegistryConfig(
        registry_path=os.getenv("MODEL_REGISTRY_PATH", "models/registry"),
        max_history=int(os.getenv("MODEL_REGISTRY_MAX_HISTORY", "10")),
        promotion_threshold=int(os.getenv("MODEL_PROMOTION_THRESHOLD", "2")),
    )
