"""
Centralized logging configuration for EX05 ML Pipeline.
Provides consistent logging format across all modules.

Usage:
    from src.logging_config import get_logger
    logger = get_logger(__name__)
    logger.info("Message")
"""

import logging
import sys
from pathlib import Path
from datetime import datetime


# Global configuration
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_LEVEL = logging.INFO


def get_logger(name: str, level: int = DEFAULT_LEVEL) -> logging.Logger:
    """
    Get a configured logger instance.
    
    Args:
        name: Logger name (typically __name__)
        level: Logging level (default: INFO)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Avoid adding handlers multiple times
    if not logger.handlers:
        logger.setLevel(level)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
        logger.addHandler(console_handler)
        
        # Prevent propagation to avoid duplicate logs
        logger.propagate = False
    
    return logger


def configure_file_logging(
    logger: logging.Logger,
    log_dir: str = "logs",
    filename_prefix: str = "ml_pipeline"
) -> logging.Logger:
    """
    Add file logging to an existing logger.
    
    Args:
        logger: Logger instance to configure
        log_dir: Directory for log files
        filename_prefix: Prefix for log filename
    
    Returns:
        Logger with file handler added
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_path / f"{filename_prefix}_{timestamp}.log"
    
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)  # Capture all levels to file
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))
    
    logger.addHandler(file_handler)
    logger.info(f"File logging enabled: {log_file}")
    
    return logger


class PipelineLogger:
    """
    Structured logger for pipeline stages with metrics tracking.
    Provides standardized logging for inter-stage verification.
    """
    
    def __init__(self, pipeline_name: str):
        self.logger = get_logger(pipeline_name)
        self.metrics = {}
        self.stage_start_time = None
    
    def stage_start(self, stage_name: str, **context):
        """Log the start of a pipeline stage."""
        self.stage_start_time = datetime.now()
        context_str = " | ".join(f"{k}={v}" for k, v in context.items())
        self.logger.info(f"▶ STAGE START: {stage_name} | {context_str}")
    
    def stage_end(self, stage_name: str, row_count: int = None, **context):
        """Log the end of a pipeline stage with metrics."""
        duration = None
        if self.stage_start_time:
            duration = (datetime.now() - self.stage_start_time).total_seconds()
        
        if row_count is not None:
            self.metrics[stage_name] = {"row_count": row_count, "duration": duration}
        
        context_str = " | ".join(f"{k}={v}" for k, v in context.items())
        duration_str = f"{duration:.2f}s" if duration else "N/A"
        rows_str = f"{row_count:,}" if row_count else "N/A"
        
        self.logger.info(
            f"✓ STAGE END: {stage_name} | rows={rows_str} | duration={duration_str} | {context_str}"
        )
    
    def verify_retention(
        self,
        stage_from: str,
        stage_to: str,
        min_threshold: float = 0.80,
        warn_threshold: float = 0.90
    ) -> bool:
        """
        Verify data retention between stages.
        
        Args:
            stage_from: Source stage name
            stage_to: Target stage name
            min_threshold: Minimum acceptable retention (default 80%)
            warn_threshold: Warning threshold (default 90%)
        
        Returns:
            True if retention is acceptable, False otherwise
        """
        if stage_from not in self.metrics or stage_to not in self.metrics:
            self.logger.warning(f"Cannot verify retention: missing metrics for {stage_from} or {stage_to}")
            return True
        
        count_from = self.metrics[stage_from]["row_count"]
        count_to = self.metrics[stage_to]["row_count"]
        
        if count_from == 0:
            self.logger.error(f"⚠ RETENTION CHECK FAILED: {stage_from} has 0 rows")
            return False
        
        retention = count_to / count_from
        retention_pct = retention * 100
        
        if retention < min_threshold:
            self.logger.error(
                f"✗ RETENTION FAILED: {stage_from}→{stage_to} | "
                f"{retention_pct:.1f}% < {min_threshold*100:.0f}% minimum | "
                f"Lost {count_from - count_to:,} rows ({count_from:,} → {count_to:,})"
            )
            return False
        elif retention < warn_threshold:
            self.logger.warning(
                f"⚠ RETENTION WARNING: {stage_from}→{stage_to} | "
                f"{retention_pct:.1f}% < {warn_threshold*100:.0f}% | "
                f"Lost {count_from - count_to:,} rows ({count_from:,} → {count_to:,})"
            )
            return True
        else:
            self.logger.info(
                f"✓ RETENTION OK: {stage_from}→{stage_to} | "
                f"{retention_pct:.1f}% retained ({count_from:,} → {count_to:,})"
            )
            return True
    
    def summary(self):
        """Log a summary of all stage metrics."""
        self.logger.info("=" * 60)
        self.logger.info("PIPELINE SUMMARY")
        self.logger.info("-" * 60)
        for stage, data in self.metrics.items():
            duration_str = f"{data['duration']:.2f}s" if data.get('duration') else "N/A"
            self.logger.info(f"  {stage}: {data['row_count']:,} rows in {duration_str}")
        self.logger.info("=" * 60)


# Convenience function for quick debug logging
def log_dataframe_info(logger: logging.Logger, df, name: str = "DataFrame"):
    """Log DataFrame shape and basic info."""
    try:
        count = df.count() if hasattr(df, 'count') else len(df)
        cols = len(df.columns) if hasattr(df, 'columns') else 'N/A'
        logger.info(f"{name}: {count:,} rows x {cols} columns")
    except Exception as e:
        logger.warning(f"Could not log DataFrame info for {name}: {e}")
