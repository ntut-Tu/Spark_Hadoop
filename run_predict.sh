#!/bin/bash

# 預測資料路徑
BASE_DIR=$(dirname "$(readlink -f "$0")")
PREDICT_LOCAL_PATH="$BASE_DIR/data/predict/predict.csv"
PREDICT_HDFS_PATH="/data/predict/predict.csv"

# 確認本地檔案存在
if [ ! -f "$PREDICT_LOCAL_PATH" ]; then
  echo "❌ 找不到要預測的資料: $PREDICT_LOCAL_PATH"
  exit 1
fi

# 若 HDFS 上還沒上傳就上傳
hdfs dfs -test -e "$PREDICT_HDFS_PATH"
if [ $? -ne 0 ]; then
  echo "📤 上傳預測資料到 HDFS: $PREDICT_HDFS_PATH"
  hdfs dfs -mkdir -p "$(dirname "$PREDICT_HDFS_PATH")"
  hdfs dfs -put "$PREDICT_LOCAL_PATH" "$PREDICT_HDFS_PATH"
else
  echo "✅ HDFS 上已有預測資料，略過上傳"
fi

# 執行預測腳本
spark-submit \
  --master spark://192.168.100.10:7077 \
  --conf spark.driver.host=192.168.100.10 \
  predict.py

# 下載結果
echo "📥 從 HDFS 複製預測結果..."
hdfs dfs -get / output/

echo "✅ 預測結果已下載至 output/predict"
