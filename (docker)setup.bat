@echo off
docker build -t my-spark:latest .
docker-compose -p spark_ai up -d
pause