@echo off
chcp 65001

echo ✅ 啟動 Kafka CLI 輸入系統
echo 🐍 執行 Python 程式：send_input_cli.py

REM 切換到當前資料夾（假設 send_input_cli.py 在這裡）
cd /d %~dp0

REM 使用你環境中的 Python 版本啟動 CLI 輸入
docker exec -it spark-master python send_input_cli.py

pause
