#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
export HADOOP_HOME="${HADOOP_HOME:-$HOME/hadoop-3.2.0}"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export SPARK_HOME="${SPARK_HOME:-$HOME/spark-3.1.2-bin-hadoop3.2}"
export MOVIELENS_HDFS_BASE="${MOVIELENS_HDFS_BASE:-hdfs://namenode:9000/movie}"
"$SPARK_HOME/bin/spark-shell" --master yarn --deploy-mode client \
  --driver-memory 1g --num-executors 1 --executor-cores 1 --executor-memory 1g \
  --conf spark.sql.shuffle.partitions=16 --conf spark.sql.catalogImplementation=in-memory \
  -i "$root/pipeline/rdd/movie_similarity.scala" </dev/null
"$HADOOP_HOME/bin/hdfs" dfs -test -e "$MOVIELENS_HDFS_BASE/result/rdd/movie_similarity/_SUCCESS"
"$HADOOP_HOME/bin/hdfs" dfs -cat "$MOVIELENS_HDFS_BASE/result/rdd/data_quality/part-*.json"
