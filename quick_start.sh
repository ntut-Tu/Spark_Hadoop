#!/bin/bash
echo "⚠️ 警告：這個操作將刪除 HDFS 上的資料（/data、/user/master/output、/models）！"
echo "若只是要執行 Spark，請改用 ./run_spark.sh"
read -p "你確定要繼續嗎？(y/n): " confirm

if [[ "$confirm" != "y" ]]; then
  echo "❌ 操作已取消"
  exit 1
fi

./clean_hdfs.sh
./clean_local.sh
./run_spark.sh