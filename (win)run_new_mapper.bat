@echo off
echo WIP !
chcp 65001
set BASE_DIR=%~dp0
set DATA_PATH=%BASE_DIR%data\output\full

if not exist %DATA_PATH%\*.parquet (
 echo ❌ 找不到資料: %DATA_PATH%\*.parquet
 exit /b 1
)

echo 🚀 開始執行 UMAP 繪圖...
docker exec -it spark-master spark-submit ^
 --master spark://spark-master:7077 ^
 new_umap_maker.py ^
 --input %DATA_PATH% ^
 --output_dir /app/output

echo ✅ 圖片已輸出到 output\ 資料夾
pause
