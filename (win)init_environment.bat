@echo off

docker exec -it namenode hdfs dfs -mkdir -p /data/predict
docker exec -it namenode hdfs dfs -mkdir -p /data/raw
docker exec -it namenode hdfs dfs -mkdir -p /data/logging
docker exec -it namenode hdfs dfs -mkdir -p /data/interim
docker exec -it namenode hdfs dfs -mkdir -p /data/processed/final
docker exec -it namenode hdfs dfs -mkdir -p /data/full
docker exec -it namenode hdfs dfs -mkdir -p /data/processed/predict
docker exec -it namenode hdfs dfs -mkdir -p /data/output
docker exec -it namenode hdfs dfs -mkdir -p /models/score_cluster_model
docker exec -it namenode hdfs dfs -mkdir -p /models/background_cluster_model

:: for testing
docker exec -it namenode hdfs dfs -mkdir -p /testing/data/
docker exec -it namenode hdfs dfs -mkdir -p /testing/models/
docker exec -it namenode hdfs dfs -chmod -R 777 /testing
:: 修改權限
docker exec -it namenode hdfs dfs -chmod -R 777 /data
docker exec -it namenode hdfs dfs -chmod -R 777 /models

pause