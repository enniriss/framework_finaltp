# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("silver").getOrCreate()

# Lecture Bronze (depuis HDFS, partitionné)
bronze_path = "hdfs://namenode:9000/data/raw/breweries_partitioned"
bronze = spark.read.parquet(bronze_path)

# Nettoyage : suppression des lignes avec valeurs manquantes et doublons sur user_id
bronze_clean = bronze.dropna().dropDuplicates(["user_id"])

# Normalisation : preferred_payment_method en minuscules
if "preferred_payment_method" in bronze_clean.columns:
    bronze_clean = bronze_clean.withColumn(
        "preferred_payment_method",
        F.lower(F.col("preferred_payment_method"))
    )

# Normalisation d'autres champs
if "gender" in bronze_clean.columns:
    bronze_clean = bronze_clean.withColumn("gender", F.lower(F.col("gender")))
if "country" in bronze_clean.columns:
    bronze_clean = bronze_clean.withColumn("country", F.initcap(F.col("country")))

# Écriture Silver (toujours sur HDFS)
silver_path = "hdfs://namenode:9000/data/silver/shopping_behavior"
bronze_clean.write.mode("overwrite").parquet(silver_path)