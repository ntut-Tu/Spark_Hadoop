@echo off
chcp 65001
set BASE_DIR=%~dp0

docker exec -it spark-master spark-submit ^
    --master spark://spark-master:7077 ^
    --conf spark.driver.host=spark-master ^
    --conf spark.driver.bindAddress=0.0.0.0 ^
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4 ^
    /app/kafkas/predict_streaming.py

pause