@echo off
chcp 65001
set BASE_DIR=%~dp0
set PREDICT_LOCAL_PATH=/data/predict/predict.csv
set PREDICT_HDFS_PATH=/data/predict/predict.csv

if not exist %BASE_DIR%data/predict/predict.csv (
    echo ❌ 找不到要預測的資料: %BASE_DIR%data/predict/predict.csv
    exit /b 1
)

docker exec -it namenode hdfs dfs -test -e %PREDICT_HDFS_PATH%
if errorlevel 1 (
    echo 📤 上傳預測資料到 HDFS: %PREDICT_HDFS_PATH%
    docker exec -it namenode hdfs dfs -put /data/predict/predict.csv /data/predict/
) else (
    echo ✅ HDFS 上已有預測資料，略過上傳
)

docker exec -it spark-master spark-submit ^
    --master spark://spark-master:7077 ^
    --conf spark.driver.host=spark-master ^
    --conf spark.driver.bindAddress=0.0.0.0 ^
    --conf spark.ui.port=4040 ^
    /app/predict.py

echo fetch data from hdfs haven't been done yet :(
@REM echo 📥 從 HDFS 複製預測結果...
@REM @REM docker exec -it namenode hdfs dfs -get / output/
@REM
@REM echo ✅ 預測結果已下載至 output\predict
pause