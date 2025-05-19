@echo off
echo WIP !
@REM chcp 65001
@REM set BASE_DIR=%~dp0
@REM set PREDICT_LOCAL_PATH=%BASE_DIR%data\predict\predict.csv
@REM set PREDICT_HDFS_PATH=/data/predict/predict.csv
@REM
@REM if not exist %PREDICT_LOCAL_PATH% (
@REM     echo ❌ 找不到要預測的資料: %PREDICT_LOCAL_PATH%
@REM     exit /b 1
@REM )
@REM
@REM docker exec -it namenode hdfs dfs -test -e %PREDICT_HDFS_PATH%
@REM if errorlevel 1 (
@REM     echo 📤 上傳預測資料到 HDFS: %PREDICT_HDFS_PATH%
@REM     docker exec -it namenode hdfs dfs -mkdir -p /data/predict
@REM     docker exec -it namenode hdfs dfs -put /host_mnt/%PREDICT_LOCAL_PATH:/data/predict/predict.csv
@REM ) else (
@REM     echo ✅ HDFS 上已有預測資料，略過上傳
@REM )
@REM
@REM docker exec -it spark-master spark-submit ^
@REM     --master spark://localhost:7077 ^
@REM     predict.py
@REM
@REM echo 📥 從 HDFS 複製預測結果...
@REM docker exec -it namenode hdfs dfs -get / output/
@REM
@REM echo ✅ 預測結果已下載至 output\predict
@REM pause