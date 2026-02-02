"""
Data validation for training and inference.

We validate:
- schema presence
- basic type expectations
- impossible negative values
- datetime sanity
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Iterable
import pandas as pd


REQUIRED_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "total_amount",
]


NON_NEGATIVE_COLUMNS = [
    "passenger_count",
    "trip_distance",
    "total_amount",
]


@dataclass(frozen=True)
class ValidationResult:
    """Validation result container."""
    is_valid: bool
    errors: list[str]


def _missing_columns(df: pd.DataFrame, required: Iterable[str]) -> list[str]:
    """

    Parameters
    ----------
    df: pd.DataFrame :

    required: Iterable[str] :


    Returns
    -------

    """
    return [c for c in required if c not in df.columns]


def validate_schema(df: pd.DataFrame, require_target: bool = True) -> ValidationResult:
    """Validate presence of required columns.

    Parameters
    ----------
    df : pandas.DataFrame
        Input data.
    require_target : bool
        If True, require `total_amount` (training).
        If False, allow missing target (inference).
    df: pd.DataFrame :

    require_target: bool :
         (Default value = True)

    Returns
    -------


    """
    if require_target:
        required = REQUIRED_COLUMNS
    else:
        required = [c for c in REQUIRED_COLUMNS if c != "total_amount"]

    missing = _missing_columns(df, required)

    errors = []
    if missing:
        errors.append(f"Missing required columns: {missing}")

    return ValidationResult(is_valid=len(errors) == 0, errors=errors)


def validate_non_negative(df: pd.DataFrame) -> ValidationResult:
    """Validate that key numeric columns are non-negative.

    Parameters
    ----------
    df: pd.DataFrame :


    Returns
    -------

    """
    errors: list[str] = []
    for col in NON_NEGATIVE_COLUMNS:
        if col in df.columns:
            if (df[col] < 0).any():
                errors.append(f"Column `{col}` contains negative values.")
    return ValidationResult(is_valid=len(errors) == 0, errors=errors)


def validate_datetimes(df: pd.DataFrame) -> ValidationResult:
    """Validate datetime columns are parseable and pickup <= dropoff.

    Parameters
    ----------
    df: pd.DataFrame :


    Returns
    -------

    """
    errors: list[str] = []
    for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        if col in df.columns:
            try:
                pd.to_datetime(df[col], errors="raise")
            except Exception as exc:  # noqa: BLE001
                errors.append(
                    f"Column `{col}` cannot be parsed as datetime: {exc}"
                )

    dt_cols = {"tpep_pickup_datetime", "tpep_dropoff_datetime"}

    if not errors and dt_cols.issubset(df.columns):
        pu = pd.to_datetime(df["tpep_pickup_datetime"])
        do = pd.to_datetime(df["tpep_dropoff_datetime"])
        if (do < pu).any():
            errors.append("Found rows where dropoff is earlier than pickup.")

    return ValidationResult(is_valid=len(errors) == 0, errors=errors)


def validate_all(df: pd.DataFrame, require_target: bool = True) -> ValidationResult:
    """Run all validation checks and aggregate errors.

    Parameters
    ----------
    df: pd.DataFrame :

    require_target: bool :
         (Default value = True)

    Returns
    -------

    """
    checks = [
        validate_schema(df, require_target=require_target),
        validate_non_negative(df),
        validate_datetimes(df),
    ]
    errors: list[str] = []
    for res in checks:
        errors.extend(res.errors)
    return ValidationResult(is_valid=len(errors) == 0, errors=errors)
