from datetime import date
from pyspark.sql import SparkSession, functions as F
import time

spark = (
    SparkSession.builder
    .appName("feeder")
    .getOrCreate()
)

demographics_path = "file:///source/user_demographics.csv"
shopping_path = "file:///source/user_shopping_behavior.csv"

df_demographics = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(demographics_path)
)

df_shopping = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(shopping_path)
)

# Jointure sur 'user_id'
df = df_demographics.join(df_shopping, on="user_id", how="inner")

today = date.today()
df2 = (
    df.withColumn("year", F.lit(today.year))
      .withColumn("month", F.lit(today.month))
      .withColumn("day", F.lit(today.day))
)

df2.cache()

output_base = "hdfs://namenode:9000/data/raw/breweries_partitioned"

time.sleep(60)

(
    df2.repartition(8)
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(output_base)
)