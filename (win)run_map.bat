@echo off
echo WIP !
@REM chcp 65001
@REM set BASE_DIR=%~dp0
@REM set DATA_PATH=%BASE_DIR%data\output\full
@REM
@REM if not exist %DATA_PATH%\*.parquet (
@REM     echo ❌ 找不到資料: %DATA_PATH%\*.parquet
@REM     exit /b 1
@REM )
@REM
@REM echo 🚀 開始執行 UMAP 繪圖...
@REM docker exec -it spark-master spark-submit ^
@REM     --master spark://localhost:7077 ^
@REM     pca.py ^
@REM     --input %DATA_PATH% ^
@REM     --output_dir %BASE_DIR%output\
@REM
@REM echo ✅ 圖片已輸出到 output\ 資料夾
@REM pause
