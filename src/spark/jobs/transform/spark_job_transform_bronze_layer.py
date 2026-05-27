import os
import re
import logging
from typing import Optional
from argparse import ArgumentParser
from functools import reduce
from py4j.protocol import Py4JJavaError


from spark.spark_session import create_spark_session
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def setup_parser():
    parser = ArgumentParser(description="Bronze Layer Ingestion Job")

    parser.add_argument("--from-bucket", required=True, help="Bucket de origem (raw)")

    parser.add_argument("--to-bucket", required=True, help="Bucket de destino (bronze)")

    parser.add_argument(
        "--data-relative-path",
        required=True,
        help="Caminho do arquivo dentro do bucket (ex: olist/orders.csv)",
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


def validate_data_relative_path(value: str, logger: logging.Logger) -> None:

    logger.info(f"Validando path: '{value}'")

    pattern = r"^[a-z0-9_-]+/[a-z0-9_.-]+\.(csv|parquet)$"

    if not re.match(pattern, value):
        logger.error(
            f"Path inválido recebido: '{value}'. " "Esperado: domain/file.(csv|parquet)"
        )
        raise ValueError(
            f"Path inválido: '{value}'. "
            "Use o formato: domain/file.(csv|parquet) "
            "Ex: olist/olist_orders_dataset.csv"
        )

    logger.info(f"Path válido: '{value}'")


def validate_dataframe(df: DataFrame, logger: logging.Logger) -> None:

    logger.info("Validando DataFrame...")

    # 1. Verifica erro de leitura
    try:
        first = df.take(1)
    except Py4JJavaError as e:
        raise ValueError(
            "Erro ao acessar o DataFrame. "
            "Verifique se o schema e o path estão corretos."
        ) from e

    # 2. Verifica se está vazio
    if not first:
        raise ValueError("DataFrame está vazio.")

    # 3. Verifica se todas as linhas são nulas ou vazias
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


def read_data(
    spark_session: SparkSession,
    source_path: str,
    logger: logging.Logger,
    schema: Optional[StructType] = None,
) -> DataFrame:

    data_extension = source_path.split(".")[-1].lower()

    try:
        if data_extension != "csv":
            raise ValueError(
                f"Formato '{data_extension}' não suportado na camada bronze. "
                "Apenas arquivos CSV são aceitos como entrada."
            )

        reader = (
            spark_session.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
        )

        if schema is not None:
            reader = reader.schema(schema)

        df = reader.load(source_path)

        validate_dataframe(df, logger)

        logger.info("Schema preview:")
        df.printSchema()
        logger.info(f"Total rows lidos: {df.count()}")

        return df

    except Exception as e:
        error_msg = str(e).lower()

        if "nosuchbucket" in error_msg or "unknownstoreexception" in error_msg:
            logger.error(
                f"Bucket não encontrado para o path '{source_path}'. "
                "Verifique se o bucket existe no MinIO."
            )
        elif "nosuchkey" in error_msg or "filenotfoundexception" in error_msg:
            logger.error(
                f"Arquivo não encontrado: '{source_path}'. "
                "Verifique se o path está correto e se o arquivo existe."
            )
        elif "accessdenied" in error_msg:
            logger.error(
                f"Acesso negado ao path '{source_path}'. "
                "Verifique as credenciais do MinIO."
            )
        else:
            logger.error(f"Erro ao ler dados de '{source_path}': {e}")

        raise


def write_data(
    df: DataFrame,
    target_path: str,
    logger: logging.Logger,
) -> None:

    df.write.mode("overwrite").option("compression", "snappy").partitionBy(
        "ingestion_year", "ingestion_month"
    ).parquet(target_path)

    logger.info(f"Dados escritos em parquet: {target_path}")


def normalize_columns(df: DataFrame) -> DataFrame:

    def normalize(col_name: str) -> str:
        col_name = col_name.lower().strip()
        col_name = re.sub(r"[^a-z0-9]+", "_", col_name)
        col_name = re.sub(r"_+", "_", col_name)
        return col_name.strip("_")

    new_columns = [normalize(c) for c in df.columns]
    return df.toDF(*new_columns)


def deduplicate(
    df: DataFrame, logger: logging.Logger, subset: list = None
) -> DataFrame:

    before = df.count()
    df = df.dropDuplicates(subset)
    after = df.count()
    removed = before - after

    if removed > 0:
        logger.warning(
            f"Deduplicação: {removed} linhas duplicadas removidas ({before} -> {after})"
        )
    else:
        logger.info("Deduplicação: nenhuma duplicata encontrada.")

    return df


def log_quality_metrics(df: DataFrame, logger: logging.Logger) -> None:

    total = df.count()
    logger.info(f"Total rows após transformações: {total}")

    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        if null_count > 0:
            pct = round(null_count / total * 100, 2)
            logger.warning(f"Coluna '{col}': {null_count} nulos ({pct}%)")

    logger.info("Métricas de qualidade concluídas.")


def write_data(
    df: DataFrame,
    target_path: str,
    logger: logging.Logger,
) -> None:

    df.write.mode("overwrite").option("header", "true").csv(target_path)

    logger.info(f"Dados escritos em CSV: {target_path}")


def verify_write(
    spark: SparkSession, target_path: str, expected_count: int, logger: logging.Logger
) -> None:

    written = spark.read.option("header", "true").csv(target_path).count()

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
        data_relative_path = args.data_relative_path

        # Validando path
        logger.info("Validando path...")
        validate_data_relative_path(data_relative_path, logger)
        logger.info(f"Path validado: {data_relative_path}")

        logger.info("Inicializando Spark...")
        spark = create_spark_session()
        logger.info(f"Spark inicializado: {spark.version}")

        source_path = f"s3a://{from_bucket}/{data_relative_path}"
        target_path = f"s3a://{to_bucket}/{data_relative_path}"

        logger.info(f"Source: {source_path}")
        logger.info(f"Target: {target_path}")

        # Leitura
        logger.info("Lendo dados...")
        df = read_data(spark, source_path, logger)

        # Transformações
        logger.info("Normalizando colunas...")
        df = normalize_columns(df)

        logger.info("Adicionando metadados de ingestão...")
        df = df.withColumn("ingestion_date", F.current_timestamp())
        df = df.withColumn("ingestion_year", F.year("ingestion_date"))
        df = df.withColumn("ingestion_month", F.month("ingestion_date"))

        # Cache antes das operações que fazem múltiplos counts
        df.cache()
        logger.info("DataFrame em cache.")

        # Deduplicação
        logger.info("Verificando duplicatas...")
        df = deduplicate(df, logger)

        # Métricas de qualidade
        logger.info("Calculando métricas de qualidade...")
        log_quality_metrics(df, logger)

        expected_count = df.count()

        # Escrita
        logger.info("Escrevendo dados em Parquet...")
        write_data(df, target_path, logger)

        # Libera cache após escrita
        df.unpersist()
        logger.info("Cache liberado.")

        # Verificação
        logger.info("Verificando escrita...")
        verify_write(spark, target_path, expected_count, logger)

        logger.info("Pipeline finalizado com sucesso!")

    except Exception as e:
        logger.error(f"Pipeline falhou: {e}")
        raise

    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession encerrada.")


if __name__ == "__main__":
    main()
