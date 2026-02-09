# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("silver_join_process").getOrCreate()

# 1. Lecture des deux sources Bronze
df_info = spark.read.parquet("hdfs://namenode:9000/data/raw/user_info1")
df_activity = spark.read.parquet("hdfs://namenode:9000/data/raw/user_info2")

# --- CORRECTION DE L'AMBIGUÏTÉ ---
# On supprime les colonnes de partitionnement de la deuxième table 
# si elles existent déjà dans la première.
common_cols_to_drop = ["year", "month", "day"] 
for col_name in common_cols_to_drop:
    if col_name in df_activity.columns and col_name in df_info.columns:
        df_activity = df_activity.drop(col_name)

# 2. Jointure
# En utilisant on="user_id", Spark fusionne déjà cette colonne proprement.
# Plus simple et robuste
bronze_joined = df_info.join(df_activity.drop("last_purchase_date"), on="user_id", how="inner")


# 3. Nettoyage
# Maintenant, dropna() ne verra plus de colonnes en double
silver_df = bronze_joined.dropDuplicates(["user_id"])

# 4. Normalisation (ton code reste identique ici)
if "preferred_payment_method" in silver_df.columns:
    silver_df = silver_df.withColumn("preferred_payment_method", F.lower(F.col("preferred_payment_method")))
if "gender" in silver_df.columns:
    silver_df = silver_df.withColumn("gender", F.lower(F.col("gender")))
if "country" in silver_df.columns:
    silver_df = silver_df.withColumn("country", F.initcap(F.col("country")))



# 5. Écriture Silver
silver_path = "hdfs://namenode:9000/data/silver/user_info_cleaned"
silver_df.write.mode("overwrite").parquet(silver_path)

print("Traitement Silver terminé sans ambiguïté.")