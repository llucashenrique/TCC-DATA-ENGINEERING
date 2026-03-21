import os
from pyspark.sql import SparkSession


def create_spark_session(app_name: str = "TCC-Data-Engineering") -> SparkSession:
    
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")

    spark = (
        SparkSession.builder
        .appName(app_name)
        
        # Configuração MinIO (S3A)
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        
        # Performance básica
        .config("spark.sql.shuffle.partitions", "4")
        
        .getOrCreate()
    )

    return spark