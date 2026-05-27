import os
import logging
from argparse import ArgumentParser
from functools import reduce

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


def setup_parser():
    parser = ArgumentParser(description="Gold Layer - Dimensions and Facts Job")
    parser.add_argument(
        "--from-bucket", required=True, help="Bucket de origem (silver)"
    )
    parser.add_argument("--to-bucket", required=True, help="Bucket de destino (gold)")
    return parser.parse_args()


def read_silver(
    spark: SparkSession, bucket: str, dataset_name: str, logger: logging.Logger
) -> DataFrame:

    path = f"s3a://{bucket}/olist/{dataset_name}"
    logger.info(f"Lendo silver: {path}")
    df = spark.read.parquet(path)
    logger.info(f"{dataset_name}: {df.count()} rows")
    return df


def apply_date_rules(df: DataFrame, logger: logging.Logger) -> DataFrame:

    logger.info("Aplicando regras de datas...")

    df = df.withColumn(
        "flag_delivered_before_purchase",
        F.when(
            F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"),
            True,
        ).otherwise(False),
    )

    df = df.withColumn(
        "flag_estimated_before_purchase",
        F.when(
            F.col("order_estimated_delivery_date") < F.col("order_purchase_timestamp"),
            True,
        ).otherwise(False),
    )

    df = df.withColumn(
        "flag_future_purchase",
        F.when(
            F.col("order_purchase_timestamp") > F.current_timestamp(), True
        ).otherwise(False),
    )

    invalid_dates = df.filter(
        F.col("flag_future_purchase")
        | F.col("flag_delivered_before_purchase")
        | F.col("flag_estimated_before_purchase")
    ).count()

    if invalid_dates > 0:
        logger.warning(
            f"Encontrados {invalid_dates} pedidos com datas inválidas (flagged)."
        )

    logger.info("Regras de datas aplicadas.")
    return df


def apply_financial_rules(
    df_orders: DataFrame,
    df_payments: DataFrame,
    df_items: DataFrame,
    logger: logging.Logger,
) -> DataFrame:

    logger.info("Aplicando regras financeiras...")

    # Soma pagamentos por pedido
    payments_agg = (
        df_payments.filter(F.col("payment_value") >= 0)
        .groupBy("order_id")
        .agg(F.sum("payment_value").alias("total_payment"))
    )

    # Soma itens + frete por pedido
    items_agg = (
        df_items.filter((F.col("price") >= 0) & (F.col("freight_value") >= 0))
        .groupBy("order_id")
        .agg(
            F.sum("price").alias("total_items"),
            F.sum("freight_value").alias("total_freight"),
            (F.sum("price") + F.sum("freight_value")).alias("total_items_freight"),
        )
    )

    # Join e flag de inconsistência
    df = df_orders.join(payments_agg, "order_id", "left").join(
        items_agg, "order_id", "left"
    )

    df = df.withColumn(
        "flag_financial_inconsistency",
        F.when(
            F.round(F.col("total_payment"), 2)
            != F.round(F.col("total_items_freight"), 2),
            True,
        ).otherwise(False),
    )

    inconsistencies = df.filter(F.col("flag_financial_inconsistency")).count()
    if inconsistencies > 0:
        logger.warning(
            f"Encontrados {inconsistencies} pedidos com inconsistência financeira (flagged)."
        )

    logger.info("Regras financeiras aplicadas.")
    return df


def apply_duplicate_rules(
    df_orders: DataFrame, df_items: DataFrame, logger: logging.Logger
) -> tuple:

    logger.info("Verificando duplicatas...")

    # Duplicatas em orders
    orders_before = df_orders.count()
    df_orders = df_orders.dropDuplicates(["order_id"])
    orders_removed = orders_before - df_orders.count()
    if orders_removed > 0:
        logger.warning(f"Removidos {orders_removed} pedidos duplicados.")

    # Duplicatas em order_items (order_id + order_item_id)
    items_before = df_items.count()
    df_items = df_items.dropDuplicates(["order_id", "order_item_id"])
    items_removed = items_before - df_items.count()
    if items_removed > 0:
        logger.warning(f"Removidos {items_removed} itens duplicados.")

    logger.info("Verificação de duplicatas concluída.")
    return df_orders, df_items


def apply_customer_rules(df: DataFrame, logger: logging.Logger) -> DataFrame:

    logger.info("Aplicando regras de clientes...")

    df = df.withColumn(
        "customer_zip_code_prefix",
        F.when(
            (F.col("customer_zip_code_prefix") < 1000)
            | (F.col("customer_zip_code_prefix") > 99999),
            F.lit(None),
        ).otherwise(F.col("customer_zip_code_prefix")),
    )

    df = df.withColumn(
        "customer_city",
        F.when(F.col("customer_city").isNull(), F.lit("unknown")).otherwise(
            F.col("customer_city")
        ),
    )

    df = df.withColumn(
        "customer_state",
        F.when(F.col("customer_state").isNull(), F.lit("unknown")).otherwise(
            F.col("customer_state")
        ),
    )

    invalid_zip = df.filter(F.col("customer_zip_code_prefix").isNull()).count()
    if invalid_zip > 0:
        logger.warning(f"Encontrados {invalid_zip} clientes com CEP inválido.")

    logger.info("Regras de clientes aplicadas.")
    return df


def build_dim_customers(df: DataFrame, logger: logging.Logger) -> DataFrame:

    logger.info("Construindo dim_customers...")

    df = apply_customer_rules(df, logger)

    dim = df.select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
    ).dropDuplicates(["customer_id"])

    logger.info(f"dim_customers: {dim.count()} rows")
    return dim


def build_dim_products(
    df_products: DataFrame, df_category: DataFrame, logger: logging.Logger
) -> DataFrame:

    logger.info("Construindo dim_products...")

    dim = (
        df_products.join(df_category, "product_category_name", "left")
        .select(
            "product_id",
            "product_category_name",
            F.coalesce(
                F.col("product_category_name_english"), F.lit("uncategorized")
            ).alias("product_category_name_english"),
            "product_name_length",
            "product_description_length",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        )
        .dropDuplicates(["product_id"])
    )

    logger.info(f"dim_products: {dim.count()} rows")
    return dim


def build_dim_sellers(df: DataFrame, logger: logging.Logger) -> DataFrame:

    logger.info("Construindo dim_sellers...")

    dim = df.select(
        "seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"
    ).dropDuplicates(["seller_id"])

    logger.info(f"dim_sellers: {dim.count()} rows")
    return dim


def build_dim_dates(df_orders: DataFrame, logger: logging.Logger) -> DataFrame:

    logger.info("Construindo dim_dates...")

    dim = (
        df_orders.select(F.col("order_purchase_timestamp").alias("date"))
        .filter(F.col("date").isNotNull())
        .withColumn("date", F.to_date("date"))
        .dropDuplicates(["date"])
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("day", F.dayofmonth("date"))
        .withColumn("week", F.weekofyear("date"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("day_of_week", F.dayofweek("date"))
        .withColumn("is_weekend", F.dayofweek("date").isin([1, 7]).cast("boolean"))
    )

    logger.info(f"dim_dates: {dim.count()} rows")
    return dim


def build_fact_orders(
    df_orders: DataFrame,
    df_customers: DataFrame,
    df_payments: DataFrame,
    df_items: DataFrame,
    logger: logging.Logger,
) -> DataFrame:

    logger.info("Construindo fact_orders...")

    # Aplica regras
    df_orders = apply_date_rules(df_orders, logger)
    df_orders, df_items = apply_duplicate_rules(df_orders, df_items, logger)
    df_orders = apply_financial_rules(df_orders, df_payments, df_items, logger)

    # Métricas de entrega
    df_orders = (
        df_orders.withColumn(
            "delivery_days",
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_purchase_timestamp"),
            ),
        )
        .withColumn(
            "estimated_delivery_days",
            F.datediff(
                F.col("order_estimated_delivery_date"),
                F.col("order_purchase_timestamp"),
            ),
        )
        .withColumn(
            "flag_delayed",
            F.when(
                F.col("order_delivered_customer_date")
                > F.col("order_estimated_delivery_date"),
                True,
            ).otherwise(False),
        )
        .withColumn(
            "delay_days",
            F.when(
                F.col("flag_delayed"),
                F.datediff(
                    F.col("order_delivered_customer_date"),
                    F.col("order_estimated_delivery_date"),
                ),
            ).otherwise(0),
        )
    )

    # Join com customers para estado
    fact = df_orders.join(
        df_customers.select("customer_id", "customer_state", "customer_city"),
        "customer_id",
        "left",
    )

    fact = fact.select(
        "order_id",
        "customer_id",
        "customer_state",
        "customer_city",
        "order_status",
        "order_purchase_timestamp",
        F.to_date("order_purchase_timestamp").alias("purchase_date"),
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "total_payment",
        "total_items",
        "total_freight",
        "total_items_freight",
        "delivery_days",
        "estimated_delivery_days",
        "flag_delayed",
        "delay_days",
        "flag_financial_inconsistency",
        "flag_delivered_before_purchase",
        "flag_estimated_before_purchase",
        "flag_future_purchase",
    )

    logger.info(f"fact_orders: {fact.count()} rows")
    return fact


def build_fact_order_items(
    df_items: DataFrame,
    df_products: DataFrame,
    df_sellers: DataFrame,
    logger: logging.Logger,
) -> DataFrame:

    logger.info("Construindo fact_order_items...")

    fact = (
        df_items.join(
            df_products.select("product_id", "product_category_name_english"),
            "product_id",
            "left",
        )
        .join(df_sellers.select("seller_id", "seller_state"), "seller_id", "left")
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "seller_state",
            "product_category_name_english",
            "shipping_limit_date",
            "price",
            "freight_value",
            (F.col("price") + F.col("freight_value")).alias("total_value"),
        )
    )

    logger.info(f"fact_order_items: {fact.count()} rows")
    return fact


def write_parquet(df: DataFrame, path: str, logger: logging.Logger) -> None:

    df.write.mode("overwrite").option("compression", "snappy").parquet(path)

    logger.info(f"Escrito: {path}")


def write_postgres(df: DataFrame, table: str, logger: logging.Logger) -> None:

    jdbc_url = f"jdbc:postgresql://{os.getenv('POSTGRES_HOST', 'postgres')}:5432/{os.getenv('POSTGRES_DB', 'tcc_db')}"

    df.write.format("jdbc").option("url", jdbc_url).option("dbtable", table).option(
        "user", os.getenv("POSTGRES_USER", "admin")
    ).option("password", os.getenv("POSTGRES_PASSWORD", "admin")).option(
        "driver", "org.postgresql.Driver"
    ).mode(
        "overwrite"
    ).save()

    logger.info(f"Escrito no Postgres: {table}")


def main() -> None:

    logger = setup_logger()
    args = setup_parser()
    spark = None

    try:
        from_bucket = args.from_bucket
        to_bucket = args.to_bucket

        logger.info("Inicializando Spark...")
        spark = create_spark_session()
        logger.info(f"Spark inicializado: {spark.version}")

        # Leitura silver
        df_orders = read_silver(spark, from_bucket, "olist_orders_dataset", logger)
        df_customers = read_silver(
            spark, from_bucket, "olist_customers_dataset", logger
        )
        df_products = read_silver(spark, from_bucket, "olist_products_dataset", logger)
        df_items = read_silver(spark, from_bucket, "olist_order_items_dataset", logger)
        df_payments = read_silver(
            spark, from_bucket, "olist_order_payments_dataset", logger
        )
        df_sellers = read_silver(spark, from_bucket, "olist_sellers_dataset", logger)
        df_category = read_silver(
            spark, from_bucket, "product_category_name_translation", logger
        )

        # Cache datasets mais usados
        df_orders.cache()
        df_items.cache()
        df_payments.cache()

        # Dimensões
        dim_customers = build_dim_customers(df_customers, logger)
        dim_products = build_dim_products(df_products, df_category, logger)
        dim_sellers = build_dim_sellers(df_sellers, logger)
        dim_dates = build_dim_dates(df_orders, logger)

        # Fatos
        fact_orders = build_fact_orders(
            df_orders, df_customers, df_payments, df_items, logger
        )
        fact_order_items = build_fact_order_items(
            df_items, dim_products, dim_sellers, logger
        )

        # Cache fatos para reuso
        fact_orders.cache()
        fact_order_items.cache()

        # Escrita parquet gold
        base = f"s3a://{to_bucket}/olist"
        write_parquet(dim_customers, f"{base}/dim_customers", logger)
        write_parquet(dim_products, f"{base}/dim_products", logger)
        write_parquet(dim_sellers, f"{base}/dim_sellers", logger)
        write_parquet(dim_dates, f"{base}/dim_dates", logger)
        write_parquet(fact_orders, f"{base}/fact_orders", logger)
        write_parquet(fact_order_items, f"{base}/fact_order_items", logger)

        # Escrita Postgres
        write_postgres(dim_customers, "dim_customers", logger)
        write_postgres(dim_products, "dim_products", logger)
        write_postgres(dim_sellers, "dim_sellers", logger)
        write_postgres(dim_dates, "dim_dates", logger)
        write_postgres(fact_orders, "fact_orders", logger)
        write_postgres(fact_order_items, "fact_order_items", logger)

        df_orders.unpersist()
        df_items.unpersist()
        df_payments.unpersist()
        fact_orders.unpersist()
        fact_order_items.unpersist()

        logger.info("Pipeline Gold - Dimensões e Fatos finalizado com sucesso!")

    except Exception as e:
        logger.error(f"Pipeline falhou: {e}")
        raise

    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession encerrada.")


if __name__ == "__main__":
    main()
