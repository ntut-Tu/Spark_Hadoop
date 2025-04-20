#!/bin/bash

# 路徑基底
BASE_DIR=$(dirname "$(readlink -f "$0")")
RAW_LOCAL_PATH="$BASE_DIR/data/raw/Students_Grading_Dataset.csv"
RAW_HDFS_PATH="/data/raw/Students_Grading_Dataset.csv"

# 檢查本地檔案是否存在
if [ ! -f "$RAW_LOCAL_PATH" ]; then
  echo "❌ 找不到本地資料: $RAW_LOCAL_PATH"
  exit 1
fi

# 檢查 HDFS 是否已有檔案，若無則自動上傳
hdfs dfs -test -e "$RAW_HDFS_PATH"
if [ $? -ne 0 ]; then
  echo "📤 上傳資料到 HDFS: $RAW_HDFS_PATH"
  hdfs dfs -mkdir -p "$(dirname "$RAW_HDFS_PATH")"
  hdfs dfs -put "$RAW_LOCAL_PATH" "$RAW_HDFS_PATH"
else
  echo "✅ HDFS 檔案已存在，略過上傳"
fi

# 執行 Spark 任務
spark-submit \
  --master spark://192.168.100.10:7077 \
  --conf spark.driver.host=192.168.100.10 \
  main.py

echo "📥 將 HDFS 結果下載到本地..."

# 建立本地資料夾
mkdir -p output/interim output/processed output/crosstab

# 從 HDFS 複製 parquet 結果
hdfs dfs -get / output/

# 複製交叉表結果（csv 是目錄形式）
hdfs dfs -getmerge /data/processed/cluster_crosstab.csv output/crosstab/cluster_crosstab.csv

echo "✅ 結果已保存至本地 output/ 資料夾"
