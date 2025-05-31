@echo off
chcp 65001
set BASE_DIR=%~dp0

docker exec -it spark-master python3 -m pytest /app/kafkas/test_predict_streaming.py -s

docker exec -it spark-master python3 -m pytest /app/test_e2e_prediction.py -s

pause