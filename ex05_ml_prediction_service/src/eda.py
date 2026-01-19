"""
Mini EDA script (no notebook) for a multi-month range.

Produces:
- reports/eda_summary.json
- reports/eda_sample.csv
"""
from __future__ import annotations

import json
import os

from .config import load_data_range_config, load_minio_config, load_paths_config
from .io import read_parquet_range_from_minio
from .validation import validate_all


def main() -> None:
    """Run lightweight profiling and write reports to disk."""
    cfg_minio = load_minio_config()
    cfg_range = load_data_range_config()
    cfg_paths = load_paths_config()

    os.makedirs(cfg_paths.reports_dir, exist_ok=True)

    df = read_parquet_range_from_minio(cfg_minio, cfg_range)

    res = validate_all(df, require_target=True)

    summary = {
        "range": {
            "start_year": cfg_range.start_year,
            "start_month": cfg_range.start_month,
            "end_year": cfg_range.end_year,
            "end_month": cfg_range.end_month,
        },
        "rows": int(df.shape[0]),
        "cols": int(df.shape[1]),
        "validation_ok": res.is_valid,
        "validation_errors": res.errors,
        "missing_ratio_by_col": (
            df.isna()
            .mean()
            .sort_values(ascending=False)
            .to_dict()
        ),
        "target_desc": df["total_amount"].describe().to_dict(),
    }

    with open(
        os.path.join(
            cfg_paths.reports_dir,
            "eda_summary.json",
        ),
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(summary, f, indent=2)

    df.sample(min(2000, len(df)), random_state=42).to_csv(
        os.path.join(cfg_paths.reports_dir, "eda_sample.csv"),
        index=False,
    )

    print("EDA report written to:", cfg_paths.reports_dir)
    if not res.is_valid:
        print("WARNING: validation failed:", res.errors)


if __name__ == "__main__":
    main()
