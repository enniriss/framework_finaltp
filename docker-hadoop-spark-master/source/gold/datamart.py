# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

# Initialisation de la session avec support Hive
spark = SparkSession.builder \
    .appName("Creation-Gold-Datamarts") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

# 1. Création de la base de données GOLD si elle n'existe pas
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

# 2. Lecture de tes données Silver (depuis ton chemin spécifique)
path_silver = "hdfs://namenode:9000/data/silver/user_info_cleaned"
df_silver = spark.read.parquet(path_silver)

# --- DATAMART 1 : PREDICTION RETURN RATE ---
cols_returns = [
    'user_id', 'age', 'income_level', 'average_order_value', 
    'cart_abandonment_rate', 'return_rate'
]
df_returns = df_silver.select(*cols_returns)
# Enregistrement forcé dans la base gold


df_returns.write.mode("overwrite").saveAsTable("gold.predictive_returns")

# --- DATAMART 2 : PERSONAE ---
cols_personae = [
    'user_id', 'gender', 'ethnicity', 'social_media_influence_score', 
    'loyalty_program_member'
]
df_personae = df_silver.select(*cols_personae)
df_personae.write.mode("overwrite").saveAsTable("gold.customer_segmentation")
print("dfdf", df_silver)
print("Succes : Les datamarts ont ete crees dans la base 'gold' !")