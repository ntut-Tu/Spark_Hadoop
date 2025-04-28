#!/bin/bash

# 定位到目前這個.sh所在資料夾
BASE_DIR=$(dirname "$(readlink -f "$0")")
DATA_PATH="$BASE_DIR/data/output/full"

# 檢查資料夾下是否有parquet檔
if [ -z "$(ls "$DATA_PATH"/*.parquet 2>/dev/null)" ]; then
  echo "❌ 找不到資料: $DATA_PATH/*.parquet"
  exit 1
fi

# 執行畫圖腳本
echo "🚀 開始執行 UMAP 繪圖..."
spark-submit \
  --master spark://192.168.100.10:7077 \
  --conf spark.driver.host=192.168.100.10 \
  pca.py \
  --input "$DATA_PATH" \
  --output_dir "$BASE_DIR/output/"

echo "✅ 圖片已輸出到 output/ 資料夾"
