# -*- coding: utf-8 -*-
import sys
import os
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, filename="logs_datamart.txt")
logger = logging.getLogger(__name__)

def _is_uri(path):
    return path.startswith("hdfs://") or path.startswith("file://")

def _with_hdfs_base(path):
    if _is_uri(path):
        return path
    hdfs_base = os.getenv("HDFS_BASE", "hdfs://namenode:9000")
    if path.startswith("/"):
        return hdfs_base.rstrip("/") + path
    return hdfs_base.rstrip("/") + "/" + path

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

def _resolve_silver_path(spark, input_silver):
    if input_silver:
        return _with_hdfs_base(input_silver)

    silver_base = os.getenv("SILVER_BASE", "/data/silver")
    silver_base = _with_hdfs_base(silver_base)
    # si un dossier user_info_cleaned existe, on le privilégie
    try:
        subdirs = _list_hdfs_subdirs(spark, silver_base)
        for d in subdirs:
            if d.rstrip("/").endswith("/user_info_cleaned"):
                return d
        # sinon, prendre le premier dossier trouvé
        if subdirs:
            return sorted(subdirs)[0]
    except Exception:
        pass

    return silver_base

def _resolve_gold_path():
    gold_base = os.getenv("GOLD_BASE", "/data/gold")
    return _with_hdfs_base(gold_base)

def create_gold(input_silver):
    spark = SparkSession.builder \
        .appName("Gold-Datamarts") \
        .enableHiveSupport() \
        .getOrCreate()

    input_silver = _resolve_silver_path(spark, input_silver)
    gold_base = _resolve_gold_path()

    logger.info("Lecture des données Silver: %s" % input_silver)
    df_silver = spark.read.parquet(input_silver)

    # Création de la DB Gold dans Hive
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")

    # DATAMART 1 : Segmentation (Relationnel Hive)
    df_personae = df_silver.select('user_id', 'gender', 'country', 'rank_in_country')
    df_personae.write.mode("overwrite").saveAsTable("gold.customer_segmentation")

    # DATAMART 2 : Performance (Parquet pour l'API)
    output_returns = gold_base.rstrip("/") + "/predictive_returns"
    df_returns = df_silver.select('user_id', 'age', 'income_level', 'return_rate')
    df_returns.write.mode("overwrite").parquet(output_returns)

    logger.info("Datamarts créés dans Hive et HDFS")

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else None
    create_gold(path)