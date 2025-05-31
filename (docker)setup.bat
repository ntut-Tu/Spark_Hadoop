@echo off
setlocal enabledelayedexpansion

docker build -t my-spark:latest .

cd web/frontend-predict/
docker build -t frontend-predict:latest .
cd ../../

docker-compose -p spark_ai up -d
pause