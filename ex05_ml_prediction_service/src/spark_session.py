from pyspark.sql import SparkSession


def create_spark_session(app_name: str) -> SparkSession:
    """

    Parameters
    ----------
    app_name: str :


    Returns
    -------

    """
    return (
        SparkSession.builder
        .appName(app_name)

        # S3A / MinIO
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # 🔑 IMPORTANT : lire les creds depuis l'environnement
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
        )

        .getOrCreate()
    )
