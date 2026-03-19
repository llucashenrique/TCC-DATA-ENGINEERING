import argparse
import logging
from spark.spark_session import create_spark_session


def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger("process_olist_dataset")


def parse_args():
    parser = argparse.ArgumentParser(description="Process Olist Dataset")

    parser.add_argument(
        "--from-bucket",
        required=True,
        help="Bucket de origem (RAW)"
    )

    parser.add_argument(
        "--to-bucket",
        required=True,
        help="Bucket de destino (CURATED)"
    )

    parser.add_argument(
        "--data-relative-path",
        required=True,
        help="Caminho do dataset dentro do bucket"
    )

    return parser.parse_args()


def read_dataset(spark, source_path, logger):

    logger.info(f"Lendo dataset: {source_path}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(source_path)
    )

    logger.info(f"Total de registros: {df.count()}")

    return df


def transform_dataset(df, logger):

    logger.info("Iniciando transformação do dataset")

    # exemplo simples de limpeza
    df_clean = df.dropDuplicates()

    return df_clean


def write_dataset(df, target_path, logger):

    logger.info(f"Gravando dataset em: {target_path}")

    (
        df.write
        .mode("overwrite")
        .parquet(target_path)
    )


def main():

    logger = setup_logger()

    args = parse_args()

    source_bucket = args.from_bucket
    target_bucket = args.to_bucket
    relative_path = args.data_relative_path

    source_path = f"s3a://{source_bucket}/{relative_path}"

    target_path = f"s3a://{target_bucket}/{relative_path.replace('.csv', '.parquet')}"

    spark = create_spark_session()

    df = read_dataset(spark, source_path, logger)

    df_transformed = transform_dataset(df, logger)

    write_dataset(df_transformed, target_path, logger)

    spark.stop()

    logger.info("Processamento finalizado com sucesso")


if __name__ == "__main__":
    main()