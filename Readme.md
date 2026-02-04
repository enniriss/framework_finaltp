# Lancer HDFS

``` bash
cd docker-hadoop-spark
```

``` bash
dokcer-compose up -d
```

``` bash
docker cp ./data/user_demographics.csv namenode:demographics.csv
docker cp ./data/user_shopping_behavior.csv namenode:shopping_behavior.csv
```

``` bash
docker exec -it namenode bash
```

``` bash
hdfs dfs -mkdir -p /data/openbeer/demographics
hdfs dfs -mkdir -p /data/openbeer/shopping_behavior
```

``` bash
hdfs dfs -put demographics.csv /data/openbeer/demographics/demographics.csv
hdfs dfs -put shopping_behavior.csv /data/openbeer/shopping_behavior/shopping_behavior.csv
```

# Lancer Spark

``` bash
docker exec -it spark-master bash
```

``` bash
/spark/bin/pyspark --master spark://spark-master:7077
```

``` bash
demographics = spark.read.csv("hdfs://namenode:9000/data/openbeer/demographics/demographics.csv")
shopping_behavior = spark.read.csv("hdfs://namenode:9000/data/openbeer/shopping_behavior/shopping_behavior.csv")
```