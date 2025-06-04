@echo off
chcp 65001

docker exec -it namenode hdfs dfs -get /data /data
docker cp namenode:/data ".\output"

pause