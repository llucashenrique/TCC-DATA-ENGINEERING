import os
import re
import logging
from functools import reduce
from argparse import ArgumentParser
from typing import Optional

from spark.spark_session import create_spark_session
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)
from py4j.protocol import Py4JJavaError

SCHEMAS = {
    "olist_orders_dataset": StructType(
        [
            StructField("order_id", StringType(), nullable=False),
            StructField("customer_id", StringType(), nullable=False),
            StructField("order_status", StringType(), nullable=False),
            StructField("order_purchase_timestamp", TimestampType(), nullable=False),
            StructField("order_approved_at", TimestampType(), nullable=True),
            StructField("order_delivered_carrier_date", TimestampType(), nullable=True),
            StructField(
                "order_delivered_customer_date", TimestampType(), nullable=True
            ),
            StructField(
                "order_estimated_delivery_date", TimestampType(), nullable=True
            ),
        ]
    ),
    "olist_customers_dataset": StructType(
        [
            StructField("customer_id", StringType(), nullable=False),
            StructField("customer_unique_id", StringType(), nullable=False),
            StructField("customer_zip_code_prefix", IntegerType(), nullable=False),
            StructField("customer_city", StringType(), nullable=False),
            StructField("customer_state", StringType(), nullable=False),
        ]
    ),
    "olist_products_dataset": StructType(
        [
            StructField("product_id", StringType(), nullable=False),
            StructField("product_category_name", StringType(), nullable=True),
            StructField("product_name_length", IntegerType(), nullable=True),
            StructField("product_description_length", IntegerType(), nullable=True),
            StructField("product_photos_qty", IntegerType(), nullable=True),
            StructField("product_weight_g", IntegerType(), nullable=True),
            StructField("product_length_cm", IntegerType(), nullable=True),
            StructField("product_height_cm", IntegerType(), nullable=True),
            StructField("product_width_cm", IntegerType(), nullable=True),
        ]
    ),
    "olist_order_items_dataset": StructType(
        [
            StructField("order_id", StringType(), nullable=False),
            StructField("order_item_id", IntegerType(), nullable=False),
            StructField("product_id", StringType(), nullable=False),
            StructField("seller_id", StringType(), nullable=False),
            StructField("shipping_limit_date", TimestampType(), nullable=True),
            StructField("price", DoubleType(), nullable=False),
            StructField("freight_value", DoubleType(), nullable=False),
        ]
    ),
    "olist_order_payments_dataset": StructType(
        [
            StructField("order_id", StringType(), nullable=False),
            StructField("payment_sequential", IntegerType(), nullable=False),
            StructField("payment_type", StringType(), nullable=False),
            StructField("payment_installments", IntegerType(), nullable=False),
            StructField("payment_value", DoubleType(), nullable=False),
        ]
    ),
    "olist_order_reviews_dataset": StructType(
        [
            StructField("review_id", StringType(), nullable=False),
            StructField("order_id", StringType(), nullable=False),
            StructField("review_score", IntegerType(), nullable=False),
            StructField("review_comment_title", StringType(), nullable=True),
            StructField("review_comment_message", StringType(), nullable=True),
            StructField("review_creation_date", TimestampType(), nullable=True),
            StructField("review_answer_timestamp", TimestampType(), nullable=True),
        ]
    ),
    "olist_sellers_dataset": StructType(
        [
            StructField("seller_id", StringType(), nullable=False),
            StructField("seller_zip_code_prefix", IntegerType(), nullable=False),
            StructField("seller_city", StringType(), nullable=False),
            StructField("seller_state", StringType(), nullable=False),
        ]
    ),
    "olist_geolocation_dataset": StructType(
        [
            StructField("geolocation_zip_code_prefix", IntegerType(), nullable=False),
            StructField("geolocation_lat", DoubleType(), nullable=False),
            StructField("geolocation_lng", DoubleType(), nullable=False),
            StructField("geolocation_city", StringType(), nullable=False),
            StructField("geolocation_state", StringType(), nullable=False),
        ]
    ),
    "product_category_name_translation": StructType(
        [
            StructField("product_category_name", StringType(), nullable=False),
            StructField("product_category_name_english", StringType(), nullable=False),
        ]
    ),
}

STRICT_COLUMNS = {
    "olist_orders_dataset": [
        "order_id",
        "customer_id",
        "order_status",
        "order_purchase_timestamp",
    ],
    "olist_customers_dataset": [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ],
    "olist_products_dataset": ["product_id"],
    "olist_order_items_dataset": [
        "order_id",
        "order_item_id",
        "product_id",
        "seller_id",
        "price",
        "freight_value",
    ],
    "olist_order_payments_dataset": [
        "order_id",
        "payment_sequential",
        "payment_type",
        "payment_installments",
        "payment_value",
    ],
    "olist_order_reviews_dataset": ["review_id", "order_id", "review_score"],
    "olist_sellers_dataset": [
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
    ],
    "olist_geolocation_dataset": [
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
    ],
    "product_category_name_translation": [
        "product_category_name",
        "product_category_name_english",
    ],
}


def setup_parser():
    parser = ArgumentParser(description="Silver Layer Transformation Job")

    parser.add_argument(
        "--from-bucket", required=True, help="Bucket de origem (bronze)"
    )

    parser.add_argument("--to-bucket", required=True, help="Bucket de destino (silver)")

    parser.add_argument(
        "--dataset-name",
        required=True,
        help="Nome do dataset (ex: olist_orders_dataset)",
    )

    return parser.parse_args()


def setup_logger() -> logging.Logger:
    logger = logging.getLogger(__name__)

    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s",
            "%y/%m/%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger


def validate_dataset_name(name: str, logger: logging.Logger) -> None:

    logger.info(f"Validando dataset name: '{name}'")

    if name not in SCHEMAS:
        raise ValueError(
            f"Dataset '{name}' não reconhecido. " f"Disponíveis: {list(SCHEMAS.keys())}"
        )

    logger.info(f"Dataset name válido: '{name}'")


def validate_schema(df: DataFrame, dataset_name: str, logger: logging.Logger) -> None:

    logger.info(f"Validando schema do dataset '{dataset_name}'...")

    expected_schema = SCHEMAS[dataset_name]
    expected_columns = {field.name for field in expected_schema}
    df_columns = set(df.columns)

    missing = expected_columns - df_columns
    extra = (
        df_columns
        - expected_columns
        - {"ingestion_date", "ingestion_year", "ingestion_month"}
    )

    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing}")

    if extra:
        logger.warning(f"Colunas extras encontradas: {extra}")

    logger.info("Validação de schema concluída com sucesso.")


def remove_strict_nulls(
    df: DataFrame, dataset_name: str, logger: logging.Logger
) -> DataFrame:

    strict_cols = STRICT_COLUMNS.get(dataset_name, [])

    before = df.count()
    df = df.dropna(subset=strict_cols)
    after = df.count()
    removed = before - after

    if removed > 0:
        logger.warning(
            f"Removidas {removed} linhas com nulos em colunas críticas "
            f"({before} -> {after} rows)"
        )
    else:
        logger.info("Nenhuma linha removida por nulos críticos.")

    return df


def validate_soft_columns(
    df: DataFrame, dataset_name: str, logger: logging.Logger
) -> None:

    logger.info("Validando colunas opcionais (soft validation)...")

    expected_schema = SCHEMAS[dataset_name]
    strict_cols = set(STRICT_COLUMNS.get(dataset_name, []))
    total = df.count()

    for field in expected_schema:
        if field.nullable and field.name not in strict_cols:
            null_count = df.filter(F.col(field.name).isNull()).count()
            if null_count > 0:
                pct = round(null_count / total * 100, 2)
                logger.warning(
                    f"Coluna opcional '{field.name}': " f"{null_count} nulos ({pct}%)"
                )

    logger.info("Validação soft concluída.")


def validate_dataframe(df: DataFrame, logger: logging.Logger) -> None:

    logger.info("Validando DataFrame...")

    try:
        first = df.take(1)
    except Py4JJavaError as e:
        raise ValueError(
            "Erro ao acessar o DataFrame. "
            "Verifique se o schema e o path estão corretos."
        ) from e

    if not first:
        raise ValueError("DataFrame está vazio.")

    empty_cols = [
        (F.coalesce(F.trim(F.col(c).cast("string")), F.lit("")) == F.lit("")).alias(c)
        for c in df.columns
    ]

    flags = df.select(*empty_cols)
    all_empty_row = reduce(lambda a, b: a & b, [F.col(c) for c in flags.columns])
    has_non_empty = flags.filter(~all_empty_row).limit(1).count() > 0

    if not has_non_empty:
        raise ValueError("DataFrame contém apenas linhas nulas ou vazias.")

    logger.info("DataFrame validado com sucesso.")


def fix_column_names(
    df: DataFrame, dataset_name: str, logger: logging.Logger
) -> DataFrame:

    renames = {
        "olist_products_dataset": {
            "product_name_lenght": "product_name_length",
            "product_description_lenght": "product_description_length",
        }
    }

    if dataset_name in renames:
        for old, new in renames[dataset_name].items():
            if old in df.columns:
                df = df.withColumnRenamed(old, new)
                logger.info(f"Coluna renomeada: '{old}' -> '{new}'")

    return df


def cast_columns(df: DataFrame, dataset_name: str, logger: logging.Logger) -> DataFrame:

    logger.info("Aplicando cast de tipos...")

    schema = SCHEMAS[dataset_name]

    for field in schema:
        if field.name in df.columns:
            df = df.withColumn(field.name, F.col(field.name).cast(field.dataType))

    logger.info("Cast de tipos concluído.")
    return df


def read_data(
    spark: SparkSession, source_path: str, logger: logging.Logger
) -> DataFrame:

    logger.info(f"Lendo dados de: {source_path}")

    try:
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(source_path)
        )

        validate_dataframe(df, logger)

        logger.info("Schema preview:")
        df.printSchema()
        logger.info(f"Total rows lidos: {df.count()}")

        return df

    except Exception as e:
        error_msg = str(e).lower()

        if "nosuchbucket" in error_msg or "unknownstoreexception" in error_msg:
            logger.error(
                f"Bucket não encontrado: '{source_path}'. "
                "Verifique se o bucket existe no MinIO."
            )
        elif (
            "nosuchkey" in error_msg
            or "filenotfoundexception" in error_msg
            or "path does not exist" in error_msg
        ):
            logger.error(
                f"Arquivo não encontrado: '{source_path}'. "
                "Verifique se o path está correto."
            )
        elif "accessdenied" in error_msg:
            logger.error(
                f"Acesso negado: '{source_path}'. " "Verifique as credenciais do MinIO."
            )
        else:
            logger.error(f"Erro ao ler dados de '{source_path}': {e}")

        raise


def write_data(df: DataFrame, target_path: str, logger: logging.Logger) -> None:

    df.write.mode("overwrite").option("compression", "snappy").partitionBy(
        "ingestion_year", "ingestion_month"
    ).parquet(target_path)

    logger.info(f"Dados escritos em parquet: {target_path}")


def verify_write(
    spark: SparkSession, target_path: str, expected_count: int, logger: logging.Logger
) -> None:

    written = spark.read.parquet(target_path).count()
    logger.info(f"Verificação de escrita: {written}/{expected_count} rows escritos")

    if written != expected_count:
        raise ValueError(
            f"Write incompleto: esperado {expected_count}, escrito {written}"
        )

    logger.info("Verificação de escrita concluída com sucesso.")


def main() -> None:

    logger = setup_logger()
    args = setup_parser()
    spark = None

    try:
        from_bucket = args.from_bucket
        to_bucket = args.to_bucket
        dataset_name = args.dataset_name

        # Valida dataset name antes de qualquer coisa
        validate_dataset_name(dataset_name, logger)

        logger.info("Inicializando Spark...")
        spark = create_spark_session()
        logger.info(f"Spark inicializado: {spark.version}")

        source_path = f"s3a://{from_bucket}/olist/{dataset_name}.csv"
        target_path = f"s3a://{to_bucket}/olist/{dataset_name}"

        logger.info(f"Source: {source_path}")
        logger.info(f"Target: {target_path}")

        # Leitura
        logger.info("Lendo dados...")
        df = read_data(spark, source_path, logger)

        # Corrige nomes de colunas com typo
        logger.info("Corrigindo nomes de colunas...")
        df = fix_column_names(df, dataset_name, logger)

        # Valida schema
        logger.info("Validando schema...")
        validate_schema(df, dataset_name, logger)

        # Cast de tipos
        logger.info("Aplicando tipos corretos...")
        df = cast_columns(df, dataset_name, logger)

        # Remove linhas com nulos em colunas críticas
        logger.info("Removendo linhas com nulos em colunas críticas...")
        df = remove_strict_nulls(df, dataset_name, logger)

        # Validação suave (colunas opcionais)
        logger.info("Validando colunas opcionais...")
        validate_soft_columns(df, dataset_name, logger)

        # Cache
        df.cache()
        logger.info("DataFrame em cache.")

        expected_count = df.count()

        # Escrita
        logger.info("Escrevendo dados em Parquet...")
        write_data(df, target_path, logger)

        df.unpersist()
        logger.info("Cache liberado.")

        # Verificação
        logger.info("Verificando escrita...")
        verify_write(spark, target_path, expected_count, logger)

        logger.info("Pipeline Silver finalizado com sucesso!")

    except Exception as e:
        logger.error(f"Pipeline falhou: {e}")
        raise

    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession encerrada.")


if __name__ == "__main__":
    main()
