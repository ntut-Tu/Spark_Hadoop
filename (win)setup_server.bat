@echo off
chcp 65001
docker exec -it spark-master python3 -m uvicorn main_router:app --host 0.0.0.0 --port 8001

pause