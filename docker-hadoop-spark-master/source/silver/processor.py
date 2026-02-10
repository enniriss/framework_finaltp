# -*- coding: utf-8 -*-
import sys
import os
import logging
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

# Configuration des Logs (.txt) [cite: 36, 42]
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename="logs_processor.txt"
)
logger = logging.getLogger(__name__)

def _list_hdfs_subdirs(spark, base_path):
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
    base = jvm.org.apache.hadoop.fs.Path(base_path)
    if not fs.exists(base):
        raise RuntimeError("HDFS path not found: %s" % base_path)
    statuses = fs.listStatus(base)
    dirs = []
    for status in statuses:
        if status.isDirectory():
            dirs.append(status.getPath().toString())
    return dirs

def _is_uri(path):
    return path.startswith("hdfs://") or path.startswith("file://")

def _with_hdfs_base(path):
    if _is_uri(path):
        return path
    hdfs_base = os.getenv("HDFS_BASE", "hdfs://namenode:9000")
    if path.startswith("/"):
        return hdfs_base.rstrip("/") + path
    return hdfs_base.rstrip("/") + "/" + path

def _resolve_paths(spark, input_raw_1, input_raw_2, output_silver):
    if input_raw_1 and input_raw_2 and output_silver:
        return _with_hdfs_base(input_raw_1), _with_hdfs_base(input_raw_2), _with_hdfs_base(output_silver)

    raw_base = _with_hdfs_base(os.getenv("RAW_BASE", "/data/raw"))
    silver_base = _with_hdfs_base(os.getenv("SILVER_BASE", "/data/silver"))
    raw_dirs = _list_hdfs_subdirs(spark, raw_base)
    if not raw_dirs:
        raise RuntimeError("No datasets found under %s" % raw_base)

    def _pick_dir(name):
        for d in raw_dirs:
            if d.rstrip("/").endswith("/" + name):
                return d
        return None

    raw_1 = _pick_dir("user_info1")
    raw_2 = _pick_dir("user_info2")
    if not raw_1 or not raw_2:
        raw_dirs_sorted = sorted(raw_dirs)
        if len(raw_dirs_sorted) < 2:
            raise RuntimeError("Need at least two datasets under %s" % raw_base)
        raw_1, raw_2 = raw_dirs_sorted[0], raw_dirs_sorted[1]

    return raw_1, raw_2, silver_base


def process_silver(input_raw_1, input_raw_2, output_silver):
    spark = (
        SparkSession.builder
        .appName("silver_processor")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )
    logger.info("Démarrage du traitement Silver")

    try:
        input_raw_1, input_raw_2, output_silver = _resolve_paths(
            spark, input_raw_1, input_raw_2, output_silver
        )
        logger.info("Lecture bronze: %s , %s" % (input_raw_1, input_raw_2))
        logger.info("Ecriture silver: %s" % output_silver)
        df_info = spark.read.parquet(input_raw_1)
        df_activity = spark.read.parquet(input_raw_2)

        # 2. Jointure [cite: 47]
        # Suppression des colonnes de partition pour éviter l'ambiguïté
        df_activity_clean = df_activity.drop("year", "month", "day", "last_purchase_date")
        bronze_joined = df_info.join(df_activity_clean, on="user_id", how="inner")

        # 3. Optimisation : Utilisation de cache() [cite: 34, 50]
        bronze_joined.cache()
        logger.info("DataFrame mis en cache")

        # 4. Validation (5 règles minimum)
        # Si la colonne email n'existe pas, on ne filtre pas dessus
        has_email = "email" in bronze_joined.columns

        base_filter = (
            (F.col("age") > 0) & (F.col("age") < 120) &
            (F.col("user_id").isNotNull()) &
            (F.col("income_level") >= 0) &
            (F.col("country").isNotNull())
        )

        if has_email:
            base_filter = base_filter & F.col("email").contains("@")

        silver_df = bronze_joined.filter(base_filter)

        # 5. Window Function (Ex: Rang de dépense par pays) [cite: 16, 49]
        window_spec = Window.partitionBy("country").orderBy(F.desc("average_order_value"))
        silver_df = silver_df.withColumn("rank_in_country", F.rank().over(window_spec))

        # 6. Agrégation simple [cite: 48]
        # (Optionnel : ajout d'une colonne de moyenne globale pour démonstration)
        silver_df = silver_df.withColumn("avg_global_income", F.avg("income_level").over(Window.partitionBy()))

        # 7. Nettoyage final
        silver_df = (silver_df.dropDuplicates(["user_id"])
                     .withColumn("preferred_payment_method", F.lower(F.col("preferred_payment_method")))
                     .withColumn("country", F.initcap(F.col("country"))))

        # 8. Écriture Silver (Partitionnée) [cite: 32, 45]
        output_path = "hdfs://namenode:9000/data/silver/user_info_cleaned"
        silver_df.write.mode("overwrite").partitionBy("year", "month", "day").parquet(output_path)
        logger.info("Traitement Silver terminé avec succès vers %s" % output_path)

    except Exception as e:
        logger.error(f"Erreur durant le traitement : {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) >= 4:
        process_silver(sys.argv[1], sys.argv[2], sys.argv[3])
    else:
        process_silver(None, None, None)