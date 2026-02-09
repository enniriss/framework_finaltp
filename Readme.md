# Lancer HDFS

``` bash
cd docker-hadoop-spark
```

``` bash
dokcer-compose up -d
```

``` bash
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /source/bronze/feeder.py
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /source/silver/Clean.py
docker exec spark-master /spark/bin/spark-submit --master spark://spark-master:7077 /source/gold/datamart.py
```

/spark/bin/pyspark --master spark://spark-master:7077 --conf spark.sql.catalogImplementation=hive
spark.table("gold.predictive_returns").show(5)