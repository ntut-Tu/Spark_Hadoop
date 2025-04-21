#!/bin/bash

# === 設定 ===
WORKER_IP="192.168.100.11"
SPARK_MASTER_URL="spark://hadoop-master:7077"
SPARK_HOME="/opt/spark"
HADOOP_HOME="/opt/hadoop"

echo "===== 🚀 啟動 Hadoop + Spark Cluster ====="

# 1. 啟動 HDFS
echo "🟡 啟動 HDFS..."
$HADOOP_HOME/sbin/start-dfs.sh

# 2. 啟動 YARN
echo "🟢 啟動 YARN..."
$HADOOP_HOME/sbin/start-yarn.sh

# 3. 啟動 Spark Master（背景執行）
echo "🔵 啟動 Spark Master..."
$SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master &

# 4. 透過 ssh 啟動 Spark Worker
echo "🟣 啟動 Spark Worker on $WORKER_IP ..."
ssh -o StrictHostKeyChecking=no $WORKER_IP "$SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker $SPARK_MASTER_URL" &

echo "✅ 所有服務已嘗試啟動"
