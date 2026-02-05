from datetime import date
from pyspark.sql import SparkSession, functions as F
import time


spark = SparkSession.builder \
    .appName("test") \
    .getOrCreate()

input_path = "data/openbeer/shopping_behavior.csv" 

df_shopping_behavior = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path)
)

today = date.today()
df2 = (
    df_shopping_behavior.withColumn("year", F.lit(today.year))
      .withColumn("month", F.lit(today.month))
      .withColumn("day", F.lit(today.day))
)

df2.cache()


output_base = "./output"

time.sleep(60)

(
    df2.repartition(8)
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(output_base)
)