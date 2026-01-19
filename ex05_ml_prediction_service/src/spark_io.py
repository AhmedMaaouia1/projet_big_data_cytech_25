from pyspark.sql import DataFrame, SparkSession
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)


def check_path_exists(spark: SparkSession, path: str) -> bool:
    """
    Check if a path exists in the filesystem (S3/MinIO/local).
    
    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    path : str
        Path to check (s3a://, file://, etc.)
        
    Returns
    -------
    bool
        True if path exists
    """
    try:
        # Use Hadoop FileSystem to check existence
        hadoop_conf = spark._jsc.hadoopConfiguration()
        uri = spark._jvm.java.net.URI(path)
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, hadoop_conf)
        hadoop_path = spark._jvm.org.apache.hadoop.fs.Path(path)
        return fs.exists(hadoop_path)
    except Exception as e:
        logger.warning(f"Error checking path {path}: {e}")
        return False


def validate_months_exist(
    spark: SparkSession,
    base_path: str,
    months: List[str],
) -> Tuple[List[str], List[str]]:
    """
    Validate which months have data available.
    
    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    base_path : str
        Base path for data (e.g., s3a://nyc-interim/yellow)
    months : List[str]
        List of months to check (e.g., ["2023/01", "2023/02"])
        
    Returns
    -------
    Tuple[List[str], List[str]]
        (existing_months, missing_months)
    """
    existing = []
    missing = []
    
    for month in months:
        path = f"{base_path}/{month}"
        if check_path_exists(spark, path):
            existing.append(month)
        else:
            missing.append(month)
    
    return existing, missing


def read_multi_months(
    spark: SparkSession,
    base_path: str,
    months: List[str],
    validate: bool = True,
    fail_on_missing: bool = True,
) -> DataFrame:
    """Read multiple month partitions from MinIO.

    Example month: '2023/01'

    Parameters
    ----------
    spark : SparkSession
        Active Spark session
    base_path : str
        Base path for data
    months : List[str]
        List of months to read
    validate : bool
        Whether to validate paths exist before reading (default: True)
    fail_on_missing : bool
        If True, raise error when data is missing. 
        If False, skip missing months and continue (default: True)

    Returns
    -------
    DataFrame
        Combined DataFrame from all available months
        
    Raises
    ------
    FileNotFoundError
        If fail_on_missing=True and some months are missing
    """
    if validate:
        existing, missing = validate_months_exist(spark, base_path, months)
        
        if missing:
            missing_paths = [f"{base_path}/{m}" for m in missing]
            
            if fail_on_missing:
                raise FileNotFoundError(
                    f"Data not found for months: {missing}\n"
                    f"Missing paths:\n" + "\n".join(f"  - {p}" for p in missing_paths) + "\n"
                    f"Available months: {existing if existing else 'None'}\n"
                    f"Hint: Check if data has been ingested for these months."
                )
            else:
                logger.warning(
                    f"Skipping missing months: {missing}. "
                    f"Continuing with: {existing}"
                )
                months = existing
                
        if not months:
            raise FileNotFoundError(
                f"No data available for any of the requested months.\n"
                f"Base path: {base_path}\n"
                f"Requested months: {months}"
            )
    
    paths = [f"{base_path}/{m}" for m in months]
    logger.info(f"Reading data from {len(paths)} month(s): {months}")
    return spark.read.parquet(*paths)
