#!/usr/bin/env python
"""
ML Pipeline - Main orchestrable script for Airflow.

This script handles the complete ML workflow:
1. Load training data (sliding window of 3 months)
2. Load test data (month N)
3. Train candidate model
4. Compare with current model
5. Decide on promotion (2/3 metrics must improve)

Usage:
    # Auto-detect current month (recommended for Airflow)
    python ml_pipeline.py

    # Explicit test month
    python ml_pipeline.py --test-month 2023-06
    
    # Explicit training months
    python ml_pipeline.py --train-months 2023-03,2023-04,2023-05 --test-month 2023-06

Airflow integration:
    - Call monthly with no arguments (auto-detects current month)
    - Or pass --test-month explicitly
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from typing import List, Optional

# Add src to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from logging_config import get_logger, PipelineLogger

# Module logger
logger = get_logger(__name__)

from spark_session import create_spark_session
from spark_io import read_multi_months, validate_months_exist
from features import add_features
from trainer import train_model, save_model, generate_training_report
from error_analysis import run_error_analysis
from model_registry import (
    ModelRegistry,
    ModelMetrics,
    compute_sliding_window_months,
    parse_month_string,
    format_month_path,
    PromotionDecision,
)


DEFAULT_REGISTRY_PATH = "models/registry"
DEFAULT_DATA_BASE_PATH = "s3a://nyc-interim/yellow"
SLIDING_WINDOW_SIZE = 3
MIN_TRAIN_MONTHS = 2  # Minimum training months required


def get_current_test_month() -> str:
    """
    Get the current month as test month.
    
    For production use: returns current month in YYYY-MM format.
    Example: If today is 2026-01-06, returns "2026-01"
    
    Returns
    -------
    str
        Current month in YYYY-MM format
    """
    today = date.today()
    return f"{today.year}-{today.month:02d}"


def get_previous_month(months_back: int = 1) -> str:
    """
    Get a previous month relative to today.
    
    Parameters
    ----------
    months_back : int
        Number of months to go back (default: 1)
        
    Returns
    -------
    str
        Month in YYYY-MM format
    """
    today = date.today()
    target = today - relativedelta(months=months_back)
    return f"{target.year}-{target.month:02d}"


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="ML Pipeline for NYC Taxi fare prediction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Auto-detect current month (recommended for Airflow cron)
    python ml_pipeline.py

    # Run with explicit test month
    python ml_pipeline.py --test-month 2023-06

    # Run with explicit training months
    python ml_pipeline.py --train-months 2023-03,2023-04,2023-05 --test-month 2023-06

    # Custom registry path
    python ml_pipeline.py --test-month 2023-06 --model-registry-path /path/to/registry
        """
    )
    
    parser.add_argument(
        "--train-months",
        type=str,
        help="Comma-separated training months (e.g., 2023-03,2023-04,2023-05). "
             "If not provided, computed automatically from test-month.",
    )
    
    parser.add_argument(
        "--test-month",
        type=str,
        default=None,
        help="Test month (e.g., 2023-06). If not provided, uses current month.",
    )
    
    parser.add_argument(
        "--model-registry-path",
        type=str,
        default=DEFAULT_REGISTRY_PATH,
        help=f"Path to model registry directory (default: {DEFAULT_REGISTRY_PATH})",
    )
    
    parser.add_argument(
        "--data-base-path",
        type=str,
        default=DEFAULT_DATA_BASE_PATH,
        help=f"Base path for data in MinIO (default: {DEFAULT_DATA_BASE_PATH})",
    )
    
    parser.add_argument(
        "--reports-dir",
        type=str,
        default="reports",
        help="Directory for reports (default: reports)",
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate inputs and data availability without executing training",
    )
    
    parser.add_argument(
        "--skip-missing",
        action="store_true",
        help="Skip missing months instead of failing (use available data only)",
    )
    
    return parser.parse_args()


def parse_train_months(train_months_str: Optional[str], test_month: str) -> List[str]:
    """
    Parse training months from argument or compute from test month.
    
    Parameters
    ----------
    train_months_str : Optional[str]
        Comma-separated training months or None
    test_month : str
        Test month string
        
    Returns
    -------
    List[str]
        List of training month paths (e.g., ["2023/03", "2023/04", "2023/05"])
    """
    if train_months_str:
        # Parse explicit months
        months = []
        for m in train_months_str.split(","):
            m = m.strip()
            year, month = parse_month_string(m)
            months.append(format_month_path(year, month))
        return months
    else:
        # Compute sliding window
        return compute_sliding_window_months(test_month, SLIDING_WINDOW_SIZE)


def format_test_month_path(test_month: str) -> str:
    """Format test month string as path."""
    year, month = parse_month_string(test_month)
    return format_month_path(year, month)


def log_banner(title: str) -> None:
    """Log a banner for visibility."""
    logger.info("=" * 60)
    logger.info(f"  {title}")
    logger.info("=" * 60)


def run_pipeline(
    train_months: List[str],
    test_month: str,
    registry_path: str,
    data_base_path: str,
    reports_dir: str,
    skip_missing: bool = False,
) -> dict:
    """
    Run the complete ML pipeline.
    
    Parameters
    ----------
    train_months : List[str]
        Training month paths
    test_month : str
        Test month path
    registry_path : str
        Model registry path
    data_base_path : str
        Base path for data
    reports_dir : str
        Reports directory
    skip_missing : bool
        If True, skip missing months instead of failing
        
    Returns
    -------
    dict
        Pipeline execution result
    """
    log_banner("ML PIPELINE STARTED")
    logger.info(f"Training months: {train_months}")
    logger.info(f"Test month: {test_month}")
    logger.info(f"Registry path: {registry_path}")
    logger.info(f"Skip missing data: {skip_missing}")
    
    # Initialize pipeline logger for metrics tracking
    pipeline_log = PipelineLogger("MLPipeline")
    
    start_time = datetime.utcnow()
    
    # Initialize registry
    registry = ModelRegistry(registry_path)
    logger.info(f"Registry status: {json.dumps(registry.get_registry_summary(), indent=2)}")
    
    # Create Spark session
    log_banner("INITIALIZING SPARK")
    spark = create_spark_session("EX05-ML-Pipeline")
    
    try:
        # =====================
        # VALIDATE DATA EXISTS
        # =====================
        log_banner("VALIDATING DATA AVAILABILITY")
        
        all_months = train_months + [test_month]
        existing, missing = validate_months_exist(spark, data_base_path, all_months)
        
        logger.info(f"Requested months: {all_months}")
        logger.info(f"Available months: {existing}")
        logger.info(f"Missing months: {missing if missing else 'None'}")
        
        if missing:
            if skip_missing:
                logger.warning(f"Missing data for {missing}")
                logger.warning("Continuing with available data only (--skip-missing enabled)")
                
                # Check if test month is missing → ALWAYS FAIL
                if test_month not in existing:
                    raise FileNotFoundError(
                        f"\n❌ TEST MONTH DATA NOT FOUND\n"
                        f"{'=' * 50}\n"
                        f"Test month {test_month} has no data!\n"
                        f"Cannot proceed without test data.\n\n"
                        f"Available months: {existing}\n\n"
                        f"Solution: Use a test month that has data:\n"
                        f"  python ml_pipeline.py --test-month {existing[-1] if existing else '2023-02'}\n"
                    )
                
                # Filter train months to only existing ones
                train_months_available = [m for m in train_months if m in existing]
                
                # Check minimum training months (need at least 2)
                if len(train_months_available) < MIN_TRAIN_MONTHS:
                    raise FileNotFoundError(
                        f"\n❌ INSUFFICIENT TRAINING DATA\n"
                        f"{'=' * 50}\n"
                        f"Need at least {MIN_TRAIN_MONTHS} months of training data.\n"
                        f"Requested training months: {train_months}\n"
                        f"Available training months: {train_months_available}\n"
                        f"Only {len(train_months_available)} month(s) available.\n\n"
                        f"Options:\n"
                        f"  1. Ingest more data\n"
                        f"  2. Choose a later test month to have more training data\n\n"
                        f"Example with available data {existing}:\n"
                        f"  python ml_pipeline.py --test-month {existing[-1] if len(existing) >= 3 else 'YYYY-MM'}\n"
                    )
                
                train_months = train_months_available
                logger.info(f"Adjusted training months: {train_months} ({len(train_months)} months)")
                logger.info(f"Test month: {test_month}")
            else:
                # Fail with clear error message
                raise FileNotFoundError(
                    f"\n❌ DATA NOT FOUND\n"
                    f"{'=' * 50}\n"
                    f"Missing months: {missing}\n"
                    f"Available months: {existing if existing else 'None'}\n\n"
                    f"Options:\n"
                    f"  1. Ingest the missing data first\n"
                    f"  2. Use --skip-missing to continue with available data\n"
                    f"     (requires at least {MIN_TRAIN_MONTHS} training months + test month)\n"
                    f"  3. Use --test-month to specify a different month\n\n"
                    f"Example:\n"
                    f"  python ml_pipeline.py --test-month 2023-02\n"
                    f"  python ml_pipeline.py --test-month 2023-05 --skip-missing\n"
                )
        else:
            logger.info("All requested months have data available")
        
        # Load training data
        log_banner("LOADING TRAINING DATA")
        pipeline_log.stage_start("load_training", months=train_months)
        train_df = read_multi_months(
            spark, data_base_path, train_months, 
            validate=False  # Already validated above
        )
        train_df = add_features(train_df)
        train_count = train_df.count()
        pipeline_log.stage_end("load_training", row_count=train_count)
        
        # Load test data
        log_banner("LOADING TEST DATA")
        pipeline_log.stage_start("load_test", month=test_month)
        test_df = read_multi_months(
            spark, data_base_path, [test_month],
            validate=False  # Already validated above
        )
        test_df = add_features(test_df)
        test_count = test_df.count()
        pipeline_log.stage_end("load_test", row_count=test_count)
        
        # Train candidate model
        log_banner("TRAINING CANDIDATE MODEL")
        pipeline_log.stage_start("training")
        pipeline_log.stage_start("training")
        training_result = train_model(
            spark=spark,
            train_df=train_df,
            test_df=test_df,
            train_months=train_months,
            test_month=test_month,
        )
        pipeline_log.stage_end("training", row_count=training_result.train_rows)
        
        # Save candidate model
        candidate_path = registry.get_candidate_model_path()
        save_model(training_result.model, candidate_path)
        
        # Register candidate in registry
        registry.register_candidate(
            train_months=train_months,
            test_month=test_month,
            metrics=training_result.metrics,
            train_rows=training_result.train_rows,
            test_rows=training_result.test_rows,
        )
        
        # Compare with current model
        log_banner("MODEL COMPARISON & PROMOTION DECISION")
        decision = registry.compare_models()
        
        logger.info(f"Should promote: {decision.should_promote}")
        logger.info(f"Metrics improved: {decision.metrics_improved}")
        logger.info(f"Improvement count: {decision.improvement_count}/3")
        logger.info(f"Reason: {decision.reason}")
        
        if decision.current_metrics:
            logger.info("Current model metrics:")
            logger.info(f"  RMSE: {decision.current_metrics.rmse:.4f}")
            logger.info(f"  MAE:  {decision.current_metrics.mae:.4f}")
            logger.info(f"  R²:   {decision.current_metrics.r2:.4f}")
        
        logger.info("Candidate model metrics:")
        logger.info(f"  RMSE: {decision.candidate_metrics.rmse:.4f}")
        logger.info(f"  MAE:  {decision.candidate_metrics.mae:.4f}")
        logger.info(f"  R²:   {decision.candidate_metrics.r2:.4f}")
        
        # Execute promotion decision
        if decision.should_promote:
            log_banner("PROMOTING CANDIDATE MODEL")
            registry.promote_candidate()
            logger.info("Candidate promoted to current!")
        else:
            log_banner("KEEPING CURRENT MODEL")
            registry.discard_candidate()
            logger.info("Candidate discarded - current model retained")
        
        # Run error analysis on test predictions
        log_banner("ERROR ANALYSIS")
        pipeline_log.stage_start("error_analysis")
        predictions = training_result.model.transform(test_df)
        predictions_count = predictions.count()
        error_analysis_results = run_error_analysis(predictions, reports_dir)
        pipeline_log.stage_end("error_analysis", row_count=predictions_count)
        
        # Generate report
        promotion_info = {
            "decision": "promoted" if decision.should_promote else "discarded",
            "metrics_improved": decision.metrics_improved,
            "improvement_count": decision.improvement_count,
            "reason": decision.reason,
        }
        
        if decision.current_metrics:
            promotion_info["current_model_metrics"] = {
                "rmse": decision.current_metrics.rmse,
                "mae": decision.current_metrics.mae,
                "r2": decision.current_metrics.r2,
            }
        
        report = generate_training_report(
            result=training_result,
            promotion_decision=promotion_info,
            reports_dir=reports_dir,
        )
        
        # Add error analysis to report
        report["error_analysis"] = {
            "summary": error_analysis_results.get("error_summary", {}),
            "business_insights": error_analysis_results.get("business_insights", []),
        }
        
        # Save updated report
        report_path = os.path.join(reports_dir, "train_metrics.json")
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        
        # Log pipeline summary with retention checks
        pipeline_log.summary()
        
        log_banner("PIPELINE COMPLETED")
        logger.info(f"Duration: {(datetime.utcnow() - start_time).total_seconds():.1f}s")
        logger.info(f"Final registry status: {json.dumps(registry.get_registry_summary(), indent=2)}")
        
        return {
            "status": "success",
            "promoted": decision.should_promote,
            "metrics": {
                "rmse": training_result.metrics.rmse,
                "mae": training_result.metrics.mae,
                "r2": training_result.metrics.r2,
            },
            "train_months": train_months,
            "test_month": test_month,
        }
        
    finally:
        spark.stop()


def main() -> None:
    """Main entry point."""
    args = parse_args()
    
    # Auto-detect test month if not provided
    test_month = args.test_month
    if test_month is None:
        test_month = get_current_test_month()
        logger.info(f"[AUTO-DETECT] Using current month as test month: {test_month}")
    
    # Parse/compute training months
    train_months = parse_train_months(args.train_months, test_month)
    test_month_path = format_test_month_path(test_month)
    
    logger.info(f"Resolved training months: {train_months}")
    logger.info(f"Resolved test month: {test_month_path}")
    
    if args.dry_run:
        logger.info("[DRY RUN] Would execute pipeline with above parameters")
        logger.info("Validating data availability...")
        
        # Quick validation without full pipeline
        from spark_session import create_spark_session
        spark = create_spark_session("EX05-DryRun")
        try:
            all_months = train_months + [test_month_path]
            existing, missing = validate_months_exist(spark, args.data_base_path, all_months)
            logger.info("=" * 50)
            logger.info("DATA AVAILABILITY CHECK")
            logger.info("=" * 50)
            logger.info(f"Base path: {args.data_base_path}")
            logger.info(f"Requested: {all_months}")
            logger.info(f"Available: {existing if existing else 'None'}")
            logger.info(f"Missing:   {missing if missing else 'None'}")
            
            if missing:
                logger.warning("Some data is missing!")
                if args.skip_missing:
                    logger.info("   --skip-missing is enabled, pipeline would continue with available data")
                else:
                    logger.warning("   Pipeline would FAIL. Use --skip-missing to continue anyway.")
            else:
                logger.info("All data available. Pipeline ready to run.")
        finally:
            spark.stop()
        return
    
    try:
        result = run_pipeline(
            train_months=train_months,
            test_month=test_month_path,
            registry_path=args.model_registry_path,
            data_base_path=args.data_base_path,
            reports_dir=args.reports_dir,
            skip_missing=args.skip_missing,
        )
        
        # Exit with appropriate code
        if result["status"] == "success":
            sys.exit(0)
        else:
            sys.exit(1)
            
    except FileNotFoundError as e:
        logger.error(f"{e}")
        sys.exit(2)  # Special exit code for missing data


if __name__ == "__main__":
    main()
