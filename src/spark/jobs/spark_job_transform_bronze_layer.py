import os
import re
import logging
from functools import reduce
from argparse import ArgumentParser

from minio import Minio
from dotenv import load_dotenv

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

from pyspark.sql.utils import AnalysisException
from py4j.protocol import Py4JJavaError


load_dotenv()

def get_minio_client():
    endpoint = os.getenv("MINIO_ENDPOINT")
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")

    if not all([endpoint, access_key, secret_key]):
        raise Exception("Missing MinIO environment variables")

    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=False
    )

def setup_parser():
    parser = ArgumentParser(description="Bronze Layer Ingestion Job")

    parser.add_argument(
        "--source-bucket",
        required=True,
        help="Bucket de origem (raw)"
    )

    parser.add_argument(
        "--target-bucket",
        required=True,
        help="Bucket de destino (bronze)"
    )

    parser.add_argument(
        "--object-path",
        required=True,
        help="Caminho do arquivo dentro do bucket (ex: olist/orders/orders.csv)"
    )

    parser.add_argument(
        "--file-format",
        default="csv",
        choices=["csv", "parquet"],
        help="Formato do arquivo de entrada"
    )

    return parser.parse_args()

def validate_data_relative_path_format(value: str) -> str:
    """
    Valida o caminho no padrão:
    domain/dataset/yyyy/mm/file.(csv|parquet)
    """

    pattern = r"""
        ^
        [a-z0-9_-]+                # domain
        /
        [a-z0-9_-]+                # dataset
        /
        \d{4}                      # year
        /
        (0[1-9]|1[0-2])            # month (01-12)
        /
        [a-z0-9_-]+\.(csv|parquet) # file
        $
    """

    if not re.match(pattern, value, re.VERBOSE):
        raise ValueError(
            "Path inválido. Use: domain/dataset/yyyy/mm/file.(csv|parquet)"
        )

    return value

def setup_logger() -> logging.Logger:
    logger = logging.getLogger(__name__)

    if not logger.handlers:  # evita duplicação de logs
        handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s",
            "%y/%m/%d %H:%M:%S",
        )

        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger

def read_data(
    spark_session: SparkSession,
    source_path: str,
    schema: StructType | None = None
) -> DataFrame:

    data_extension = source_path.split(".")[-1].lower()

    try:
        if data_extension == "csv":
            reader = (
                spark_session.read
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
            )

            if schema is not None:
                reader = reader.schema(schema)

            df = reader.load(source_path)

        elif data_extension == "parquet":
            reader = spark_session.read

            if schema is not None:
                reader = reader.schema(schema)

            df = reader.parquet(source_path)

        else:
            raise ValueError(
                f"Formato '{data_extension}' não suportado."
            )

        # validação leve
        if df.rdd.isEmpty():
            raise ValueError("DataFrame vazio.")

        return df

    except AnalysisException as e:
        raise FileNotFoundError(
            f"Erro ao ler o arquivo: {source_path}"
        ) from e
    
def validate_dataframe(df: DataFrame) -> None:
    """
    Valida se o DataFrame:
    - não está vazio
    - possui ao menos uma linha com valores não nulos
    """

    # Verifica se está vazio
    if df.rdd.isEmpty():
        raise ValueError("DataFrame está vazio.")

    # Verifica se existe alguma linha com valor não nulo
    non_null_condition = None

    for col in df.columns:
        condition = F.col(col).isNotNull()

        non_null_condition = (
            condition if non_null_condition is None
            else non_null_condition | condition
        )

    has_valid_row = df.filter(non_null_condition).limit(1).count() > 0

    if not has_valid_row:
        raise ValueError("DataFrame contém apenas valores nulos.")
    
def normalize_columns(df: DataFrame) -> DataFrame:
    import re

    def normalize(col_name: str) -> str:
        col_name = col_name.lower().strip()
        col_name = re.sub(r"[^a-z0-9]+", "_", col_name)
        col_name = re.sub(r"_+", "_", col_name)
        return col_name.strip("_")

    new_columns = [normalize(c) for c in df.columns]

    return df.toDF(*new_columns)

def write_data(df: DataFrame, target_path: str, fmt: str = "parquet") -> None:

    writer = (
        df.write
        .mode("overwrite")
        .option("compression", "snappy")
    )

    if fmt == "parquet":
        writer.partitionBy("ingestion_year", "ingestion_month").parquet(target_path)

    elif fmt == "csv":
        writer.option("header", "true").csv(target_path)

    else:
        raise ValueError(f"Formato '{fmt}' não suportado.")

def convert_to_parquet(
    spark,
    source_path: str,
    target_path: str,
    schema=None
) -> str:
    """
    Converte qualquer formato suportado (csv/parquet/avro)
    para Parquet.
    """

    df = read_data(spark, source_path, schema=schema)

    write_data(df, target_path)

    return target_path



def main() -> None:

    logger = setup_logger()
    parser = setup_parser()
    args = parser.parse_args()

    source_bucket = args.from_bucket
    target_bucket = args.to_bucket
    data_relative_path = args.data_relative_path
    convert_to_parquet = args.convert_to_parquet

    if convert_to_parquet and target_bucket is None:
        parser.error("--convert-to-parquet requer --to-bucket.")

    # ---------------------------
    # Spark
    # ---------------------------
    logger.info("Inicializando Spark...")
    spark = create_spark_session()

    # ---------------------------
    # Paths (MinIO - S3A)
    # ---------------------------
    source_path = f"s3a://{source_bucket}/{data_relative_path}"

    if target_bucket:
        target_path = f"s3a://{target_bucket}/{data_relative_path.replace('.csv', '')}"
    else:
        target_path = None

    logger.info(f"Source: {source_path}")
    logger.info(f"Target: {target_path}")

    # ---------------------------
    # READ
    # ---------------------------
    logger.info("Lendo dados...")
    df = read_data(spark, source_path)

    # ---------------------------
    # TRANSFORM (Bronze)
    # ---------------------------
    logger.info("Transformando dados...")

    df = normalize_columns(df)

    df = df.withColumn("ingestion_date", F.current_timestamp())
    df = df.withColumn("ingestion_year", F.year("ingestion_date"))
    df = df.withColumn("ingestion_month", F.month("ingestion_date"))

    # ---------------------------
    # WRITE
    # ---------------------------
    if convert_to_parquet:
        logger.info("Escrevendo dados em Parquet...")

        (
            df.write
            .mode("overwrite")
            .partitionBy("ingestion_year", "ingestion_month")
            .parquet(target_path)
        )

    logger.info("Pipeline finalizado com sucesso!")