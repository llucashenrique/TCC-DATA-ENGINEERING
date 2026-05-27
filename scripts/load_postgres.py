df_gold = spark.read.parquet("s3a://datalake/gold/vendas/")

df_gold.write.format("jdbc").option(
    "url", "jdbc:postgresql://tcc_postgres:5432/tcc_db"
).option("dbtable", "gold_vendas").option("user", "admin").option(
    "password", "admin"
).option(
    "driver", "org.postgresql.Driver"
).mode(
    "overwrite"
).save()
