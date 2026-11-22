#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-3.2.0}"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME="${SPARK_HOME:-$HOME/spark-3.1.2-bin-hadoop3.2}"
export MOVIELENS_HDFS_BASE="${MOVIELENS_HDFS_BASE:-hdfs://namenode:9000/movie}"
"$SPARK_HOME/bin/spark-shell" --master yarn --deploy-mode client \
  --driver-memory 1g --num-executors 1 --executor-cores 1 --executor-memory 1g \
  --conf spark.sql.catalogImplementation=hive \
  --conf spark.hadoop.hive.metastore.uris=thrift://namenode:9083 \
  --conf spark.sql.shuffle.partitions=8 \
  -i "$root/pipeline/sql/advanced_analysis.scala" </dev/null
