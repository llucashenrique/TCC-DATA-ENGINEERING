import os
import logging
from argparse import ArgumentParser

from spark.spark_session import create_spark_session
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


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
    parser = ArgumentParser(description="Gold Layer - Metrics Job")
    parser.add_argument("--from-bucket", required=True)
    parser.add_argument("--to-bucket", required=True)
    return parser.parse_args()


def read_gold(
    spark: SparkSession, bucket: str, table: str, logger: logging.Logger
) -> DataFrame:

    path = f"s3a://{bucket}/olist/{table}"
    logger.info(f"Lendo gold: {path}")
    return spark.read.parquet(path)


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


def build_metrics_revenue(fact_orders: DataFrame, logger: logging.Logger) -> DataFrame:

    logger.info("Construindo metrics_revenue...")

    df = fact_orders.filter(F.col("order_status") != "canceled")

    # Receita total mensal
    monthly = (
        df.withColumn("year", F.year("order_purchase_timestamp"))
        .withColumn("month", F.month("order_purchase_timestamp"))
        .groupBy("year", "month")
        .agg(
            F.sum("total_payment").alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
            F.avg("total_payment").alias("avg_order_value"),
            F.expr("percentile_approx(total_payment, 0.5)").alias("median_order_value"),
            F.expr("percentile_approx(total_payment, 0.9)").alias("p90_order_value"),
        )
    )

    # Crescimento mensal
    window = Window.orderBy("year", "month")
    monthly = monthly.withColumn(
        "prev_month_revenue", F.lag("total_revenue", 1).over(window)
    ).withColumn(
        "monthly_growth_pct",
        F.when(
            F.col("prev_month_revenue").isNotNull() & (F.col("prev_month_revenue") > 0),
            F.round(
                (F.col("total_revenue") - F.col("prev_month_revenue"))
                / F.col("prev_month_revenue")
                * 100,
                2,
            ),
        ).otherwise(None),
    )

    logger.info(f"metrics_revenue: {monthly.count()} rows")
    return monthly


def build_metrics_revenue_by_state(
    fact_orders: DataFrame, logger: logging.Logger
) -> DataFrame:

    logger.info("Construindo metrics_revenue_by_state...")

    df = (
        fact_orders.filter(F.col("order_status") != "canceled")
        .groupBy("customer_state")
        .agg(
            F.sum("total_payment").alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
            F.avg("total_payment").alias("avg_order_value"),
        )
        .orderBy(F.desc("total_revenue"))
    )

    logger.info(f"metrics_revenue_by_state: {df.count()} rows")
    return df


def build_metrics_revenue_by_category(
    fact_order_items: DataFrame, fact_orders: DataFrame, logger: logging.Logger
) -> DataFrame:

    logger.info("Construindo metrics_revenue_by_category...")

    valid_orders = fact_orders.filter(F.col("order_status") != "canceled").select(
        "order_id"
    )

    df = (
        fact_order_items.join(valid_orders, "order_id", "inner")
        .groupBy("product_category_name_english")
        .agg(
            F.sum("total_value").alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
            F.sum("price").alias("total_items_revenue"),
            F.sum("freight_value").alias("total_freight_revenue"),
        )
        .orderBy(F.desc("total_revenue"))
    )

    logger.info(f"metrics_revenue_by_category: {df.count()} rows")
    return df


def build_metrics_revenue_by_seller(
    fact_order_items: DataFrame, fact_orders: DataFrame, logger: logging.Logger
) -> DataFrame:

    logger.info("Construindo metrics_revenue_by_seller...")

    valid_orders = fact_orders.filter(F.col("order_status") != "canceled").select(
        "order_id"
    )

    df = (
        fact_order_items.join(valid_orders, "order_id", "inner")
        .groupBy("seller_id", "seller_state")
        .agg(
            F.sum("price").alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
            F.countDistinct("product_id").alias("unique_products"),
        )
        .orderBy(F.desc("total_revenue"))
    )

    logger.info(f"metrics_revenue_by_seller: {df.count()} rows")
    return df


def build_metrics_delivery(fact_orders: DataFrame, logger: logging.Logger) -> DataFrame:

    logger.info("Construindo metrics_delivery...")

    delivered = fact_orders.filter(
        (F.col("order_status") == "delivered")
        & F.col("order_delivered_customer_date").isNotNull()
    )

    df = (
        delivered.withColumn("year", F.year("order_purchase_timestamp"))
        .withColumn("month", F.month("order_purchase_timestamp"))
        .groupBy("year", "month")
        .agg(
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.avg("estimated_delivery_days").alias("avg_estimated_days"),
            F.sum(F.col("flag_delayed").cast("int")).alias("total_delayed"),
            F.count("order_id").alias("total_delivered"),
            F.avg("delay_days").alias("avg_delay_days"),
        )
        .withColumn(
            "delay_rate_pct",
            F.round(F.col("total_delayed") / F.col("total_delivered") * 100, 2),
        )
        .withColumn(
            "sla_compliance_pct",
            F.round((1 - F.col("total_delayed") / F.col("total_delivered")) * 100, 2),
        )
    )

    logger.info(f"metrics_delivery: {df.count()} rows")
    return df


def build_metrics_delivery_by_state(
    fact_orders: DataFrame, logger: logging.Logger
) -> DataFrame:

    logger.info("Construindo metrics_delivery_by_state...")

    delivered = fact_orders.filter(
        (F.col("order_status") == "delivered")
        & F.col("order_delivered_customer_date").isNotNull()
    )

    df = (
        delivered.groupBy("customer_state")
        .agg(
            F.avg("delivery_days").alias("avg_delivery_days"),
            F.sum(F.col("flag_delayed").cast("int")).alias("total_delayed"),
            F.count("order_id").alias("total_delivered"),
        )
        .withColumn(
            "delay_rate_pct",
            F.round(F.col("total_delayed") / F.col("total_delivered") * 100, 2),
        )
        .orderBy(F.desc("avg_delivery_days"))
    )

    logger.info(f"metrics_delivery_by_state: {df.count()} rows")
    return df


def build_metrics_cancellations(
    fact_orders: DataFrame, logger: logging.Logger
) -> DataFrame:

    logger.info("Construindo metrics_cancellations...")

    df = (
        fact_orders.withColumn("year", F.year("order_purchase_timestamp"))
        .withColumn("month", F.month("order_purchase_timestamp"))
        .groupBy("year", "month")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum(F.when(F.col("order_status") == "canceled", 1).otherwise(0)).alias(
                "total_canceled"
            ),
            F.sum(
                F.when(
                    F.col("order_status") == "canceled", F.col("total_payment")
                ).otherwise(0)
            ).alias("lost_revenue"),
        )
        .withColumn(
            "cancellation_rate_pct",
            F.round(F.col("total_canceled") / F.col("total_orders") * 100, 2),
        )
    )

    logger.info(f"metrics_cancellations: {df.count()} rows")
    return df


def build_metrics_reviews(
    df_reviews: DataFrame,
    fact_orders: DataFrame,
    fact_order_items: DataFrame,
    logger: logging.Logger,
) -> DataFrame:

    logger.info("Construindo metrics_reviews...")

    # NPS por mês
    reviews_with_date = (
        df_reviews.join(
            fact_orders.select("order_id", "order_purchase_timestamp", "flag_delayed"),
            "order_id",
            "left",
        )
        .withColumn("year", F.year("order_purchase_timestamp"))
        .withColumn("month", F.month("order_purchase_timestamp"))
    )

    monthly_nps = (
        reviews_with_date.groupBy("year", "month")
        .agg(
            F.avg("review_score").alias("avg_score"),
            F.count("review_id").alias("total_reviews"),
            F.sum(F.when(F.col("review_score") >= 4, 1).otherwise(0)).alias(
                "promoters"
            ),
            F.sum(F.when(F.col("review_score") == 3, 1).otherwise(0)).alias("neutrals"),
            F.sum(F.when(F.col("review_score") <= 2, 1).otherwise(0)).alias(
                "detractors"
            ),
        )
        .withColumn(
            "nps",
            F.round(
                (F.col("promoters") - F.col("detractors"))
                / F.col("total_reviews")
                * 100,
                2,
            ),
        )
    )

    logger.info(f"metrics_reviews: {monthly_nps.count()} rows")
    return monthly_nps


def build_metrics_reviews_by_category(
    df_reviews: DataFrame, fact_order_items: DataFrame, logger: logging.Logger
) -> DataFrame:

    logger.info("Construindo metrics_reviews_by_category...")

    df = (
        df_reviews.join(
            fact_order_items.select("order_id", "product_category_name_english"),
            "order_id",
            "left",
        )
        .groupBy("product_category_name_english")
        .agg(
            F.avg("review_score").alias("avg_score"),
            F.count("review_id").alias("total_reviews"),
        )
        .orderBy(F.desc("avg_score"))
    )

    logger.info(f"metrics_reviews_by_category: {df.count()} rows")
    return df


def build_metrics_customers(
    fact_orders: DataFrame, logger: logging.Logger
) -> DataFrame:

    logger.info("Construindo metrics_customers...")

    # Primeira compra por cliente
    first_purchase = (
        fact_orders.groupBy("customer_id")
        .agg(
            F.min("order_purchase_timestamp").alias("first_purchase_date"),
            F.count("order_id").alias("total_orders"),
            F.sum("total_payment").alias("ltv"),
        )
        .withColumn("year", F.year("first_purchase_date"))
        .withColumn("month", F.month("first_purchase_date"))
    )

    # Novos clientes por mês
    new_customers = first_purchase.groupBy("year", "month").agg(
        F.count("customer_id").alias("new_customers"), F.avg("ltv").alias("avg_ltv")
    )

    # Clientes recorrentes (mais de 1 pedido)
    recurrent = first_purchase.filter(F.col("total_orders") > 1).count()

    total_customers = first_purchase.count()
    recurrence_rate = (
        round(recurrent / total_customers * 100, 2) if total_customers > 0 else 0
    )
    logger.info(f"Taxa de recompra: {recurrence_rate}%")

    logger.info(f"metrics_customers: {new_customers.count()} rows")
    return new_customers


def build_metrics_cohort(fact_orders: DataFrame, logger: logging.Logger) -> DataFrame:

    logger.info("Construindo metrics_cohort...")

    # Mês da primeira compra por cliente
    first_purchase = (
        fact_orders.groupBy("customer_id")
        .agg(F.min("order_purchase_timestamp").alias("first_purchase_date"))
        .withColumn(
            "cohort_month", F.date_format(F.col("first_purchase_date"), "yyyy-MM")
        )
    )

    # Join com todos os pedidos
    cohort = (
        fact_orders.join(first_purchase, "customer_id", "left")
        .withColumn(
            "order_month", F.date_format(F.col("order_purchase_timestamp"), "yyyy-MM")
        )
        .withColumn(
            "months_since_first",
            F.months_between(
                F.to_date(F.col("order_month"), "yyyy-MM"),
                F.to_date(F.col("cohort_month"), "yyyy-MM"),
            ).cast("int"),
        )
    )

    # Agrega cohort
    cohort_agg = cohort.groupBy("cohort_month", "months_since_first").agg(
        F.countDistinct("customer_id").alias("customers")
    )

    logger.info(f"metrics_cohort: {cohort_agg.count()} rows")
    return cohort_agg


def build_metrics_abc(
    fact_order_items: DataFrame, fact_orders: DataFrame, logger: logging.Logger
) -> DataFrame:

    logger.info("Construindo metrics_abc...")

    valid_orders = fact_orders.filter(F.col("order_status") != "canceled").select(
        "order_id"
    )

    # Receita por produto
    product_revenue = (
        fact_order_items.join(valid_orders, "order_id", "inner")
        .groupBy("product_id", "product_category_name_english")
        .agg(
            F.sum("price").alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
        )
    )

    # Receita total
    total_revenue = product_revenue.agg(F.sum("total_revenue")).collect()[0][0]

    # Ranking e receita acumulada
    window = Window.orderBy(F.desc("total_revenue"))

    abc = (
        product_revenue.withColumn("rank", F.row_number().over(window))
        .withColumn("revenue_pct", F.col("total_revenue") / total_revenue * 100)
        .withColumn(
            "cumulative_revenue_pct",
            F.sum("revenue_pct").over(window.rowsBetween(Window.unboundedPreceding, 0)),
        )
        .withColumn(
            "abc_class",
            F.when(F.col("cumulative_revenue_pct") <= 80, "A")
            .when(F.col("cumulative_revenue_pct") <= 95, "B")
            .otherwise("C"),
        )
    )

    class_counts = abc.groupBy("abc_class").count().collect()
    for row in class_counts:
        logger.info(f"Classe {row['abc_class']}: {row['count']} produtos")

    logger.info(f"metrics_abc: {abc.count()} rows")
    return abc


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

        # Leitura gold
        fact_orders = read_gold(spark, from_bucket, "fact_orders", logger)
        fact_order_items = read_gold(spark, from_bucket, "fact_order_items", logger)

        # Leitura silver para reviews
        df_reviews = spark.read.parquet(
            f"s3a://silver/olist/olist_order_reviews_dataset"
        )

        # Cache
        fact_orders.cache()
        fact_order_items.cache()
        df_reviews.cache()

        # Métricas de receita
        metrics_revenue = build_metrics_revenue(fact_orders, logger)
        metrics_revenue_state = build_metrics_revenue_by_state(fact_orders, logger)
        metrics_revenue_category = build_metrics_revenue_by_category(
            fact_order_items, fact_orders, logger
        )
        metrics_revenue_seller = build_metrics_revenue_by_seller(
            fact_order_items, fact_orders, logger
        )

        # Métricas de logística
        metrics_delivery = build_metrics_delivery(fact_orders, logger)
        metrics_delivery_state = build_metrics_delivery_by_state(fact_orders, logger)

        # Métricas de cancelamento
        metrics_cancellations = build_metrics_cancellations(fact_orders, logger)

        # Métricas de reviews
        metrics_reviews = build_metrics_reviews(
            df_reviews, fact_orders, fact_order_items, logger
        )
        metrics_reviews_category = build_metrics_reviews_by_category(
            df_reviews, fact_order_items, logger
        )

        # Métricas de clientes
        metrics_customers = build_metrics_customers(fact_orders, logger)
        metrics_cohort = build_metrics_cohort(fact_orders, logger)

        # Curva ABC
        metrics_abc = build_metrics_abc(fact_order_items, fact_orders, logger)

        # Escrita Postgres
        write_postgres(metrics_revenue, "metrics_revenue", logger)
        write_postgres(metrics_revenue_state, "metrics_revenue_by_state", logger)
        write_postgres(metrics_revenue_category, "metrics_revenue_by_category", logger)
        write_postgres(metrics_revenue_seller, "metrics_revenue_by_seller", logger)
        write_postgres(metrics_delivery, "metrics_delivery", logger)
        write_postgres(metrics_delivery_state, "metrics_delivery_by_state", logger)
        write_postgres(metrics_cancellations, "metrics_cancellations", logger)
        write_postgres(metrics_reviews, "metrics_reviews", logger)
        write_postgres(metrics_reviews_category, "metrics_reviews_by_category", logger)
        write_postgres(metrics_customers, "metrics_customers", logger)
        write_postgres(metrics_cohort, "metrics_cohort", logger)
        write_postgres(metrics_abc, "metrics_abc", logger)

        fact_orders.unpersist()
        fact_order_items.unpersist()
        df_reviews.unpersist()

        logger.info("Pipeline Gold - Métricas finalizado com sucesso!")

    except Exception as e:
        logger.error(f"Pipeline falhou: {e}")
        raise

    finally:
        if spark:
            spark.stop()
            logger.info("SparkSession encerrada.")


if __name__ == "__main__":
    main()
