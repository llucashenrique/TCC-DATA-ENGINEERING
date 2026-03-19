import os
from pyspark.sql import SparkSession


def create_spark_session():
    
    endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")

    spark = (
        SparkSession.builder
        .appName("TCC-Data-Engineering")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY"))
        .getOrCreate()
    )

    return spark