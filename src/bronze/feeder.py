from datetime import date
from pyspark.sql import SparkSession, functions as F
import time

spark = SparkSession.builder \
    .appName("test") \
    .getOrCreate()


input_path1 = "/opt/spark/work-dir/data/user_shopping_behavior.csv"
df_shopping_behavior = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path1)
)


input_path2 = "/opt/spark/work-dir/data/user_demographics.csv"
df_breweries = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(input_path2)
)

today = date.today()


df_shopping_behavior_clean = (
    df_shopping_behavior
    .dropDuplicates()
    .dropna()
    .withColumn("year", F.lit(today.year))
    .withColumn("month", F.lit(today.month))
    .withColumn("day", F.lit(today.day))
)


df_breweries_clean = (
    df_breweries
    .dropDuplicates()
    .dropna()
    .withColumn("year", F.lit(today.year))
    .withColumn("month", F.lit(today.month))
    .withColumn("day", F.lit(today.day))
)

df_shopping_behavior_clean.cache()
df_breweries_clean.cache()

output_base1 = "./output/shopping_behavior"
output_base2 = "./output/breweries"

time.sleep(60)

(
    df_shopping_behavior_clean.repartition(8)
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(output_base1)
)

(
    df_breweries_clean.repartition(8)
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(output_base2)
)