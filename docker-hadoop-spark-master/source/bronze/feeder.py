from datetime import date
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("bronze_loader")
    .getOrCreate()
)

# Configuration des chemins
sources = {
    "user_info1": "file:///source/user_info1.csv",
    "user_info2": "file:///source/user_info2.csv"
}
output_root = "hdfs://namenode:9000/data/raw"

for table_name, path in sources.items():
    # 1. Lecture du CSV
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    
    # 2. Conversion de la colonne date en format DateType (pour extraction)
    # On suppose que le format est YYYY-MM-DD
    df_with_date = df.withColumn("dt", F.to_date(F.col("last_purchase_date")))
    
    # 3. Creation des colonnes de partitionnement basees sur la DONNeE (pas sur l'horloge)
    # C'est ce qui cree ton historique automatiquement !
    df_final = (
        df_with_date.withColumn("year", F.year(F.col("dt")))
                    .withColumn("month", F.month(F.col("dt")))
                    .withColumn("day", F.dayofmonth(F.col("dt")))
                    .drop("dt") # On supprime la colonne temporaire
    )
    
    # 4. ecriture partitionnee
    output_path = str(output_root) + "/" + str(table_name)    
    (
        df_final.repartition(1)
        .write
        .mode("append") # On utilise append pour ne pas supprimer les jours deje existants
        .partitionBy("year", "month", "day")
        .parquet(output_path)
    )

print("Ingestion Bronze terminee : L'historique a ete reparti dans les partitions HDFS.")