from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("Olist Bronze Layer") \
    .getOrCreate()

start_time = time.time()

# Caminho dos dados Olist (local ou S3)
input_path = "s3a://olist/raw/orders.csv"

# Ler CSV raw
df = spark.read.option("header", True).csv(input_path)

# Persistir raw no MinIO (camada bronze)
output_path = "s3a://olist/bronze/orders/"
df.write.mode("overwrite").parquet(output_path)

end_time = time.time()
print(f"Bronze job finished in {end_time - start_time:.2f} seconds")