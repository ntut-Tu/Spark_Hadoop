@echo off
chcp 65001

docker exec -it namenode hdfs dfs -get /data /data
docker cp namenode:/data "E:\HW\2025_PY\Spark_AI\output"

pause