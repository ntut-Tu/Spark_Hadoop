@echo off
chcp 65001
set BASE_DIR=%~dp0
set RAW_LOCAL_PATH=/data/raw/Students_Grading_Dataset.csv
set RAW_HDFS_PATH=/data/raw/Students_Grading_Dataset.csv

:: 檢查本地資料是否存在
if not exist %BASE_DIR%data\raw\Students_Grading_Dataset.csv (
    echo ❌ 找不到本地資料: %BASE_DIR%data\raw\Students_Grading_Dataset.csv
    exit /b 1
)

docker exec -it master hdfs dfs -mkdir -p /data/predict
docker exec -it master hdfs dfs -mkdir -p /data/raw
docker exec -it master hdfs dfs -mkdir -p /data/interim
docker exec -it master hdfs dfs -mkdir -p /data/processed/final
docker exec -it master hdfs dfs -mkdir -p /data/processed/predict
docker exec -it master hdfs dfs -mkdir -p /data/full
docker exec -it master hdfs dfs -mkdir -p /data/output
docker exec -it master hdfs dfs -mkdir -p /models/score_cluster_model
docker exec -it master hdfs dfs -mkdir -p /models/background_cluster_model

:: 修改權限
docker exec -it master hdfs dfs -chmod -R 777 /data
docker exec -it master hdfs dfs -chmod -R 777 /models

:: 檢查 HDFS 是否已有檔案
docker exec -it master hdfs dfs -test -e %RAW_HDFS_PATH%
if errorlevel 1 (
    echo 📤 上傳資料到 HDFS: %RAW_HDFS_PATH%
    docker exec -it master hdfs dfs -put /data/raw/Students_Grading_Dataset.csv /data/raw/
) else (
    echo ✅ HDFS 檔案已存在，略過上傳
)

:: 提交 Spark 任務（直接使用容器內的 /app 路徑）
docker exec -it spark-client spark-submit --master yarn /app/main.py

echo fetch data from hdfs haven't been done yet :(
pause
