from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Test Postgres Connection") \
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.6.0"
    ) \
    .getOrCreate()

# DataFrame simples para teste
data = [("Lucas", 25), ("Ana", 30)]
df = spark.createDataFrame(data, ["nome", "idade"])

# Escrevendo no Postgres
df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://tcc_postgres:5432/tcc_db") \
    .option("dbtable", "teste_conexao") \
    .option("user", "admin") \
    .option("password", "admin") \
    .option("driver", "org.postgresql.Driver") \
    .mode("overwrite") \
    .save()

print("Teste concluído com sucesso!")

spark.stop()