# api_server.py
import hashlib
import os
from fastapi import FastAPI, UploadFile, HTTPException, APIRouter
from fastapi.responses import FileResponse, JSONResponse
from pyspark.sql import SparkSession

from run_batch_prediction import run_batch_prediction

router = APIRouter()
spark = SparkSession.builder.appName("BatchPredictAPI").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

DATA_DIR = "app/data/predict"
OUTPUT_DIR = "./data/processed/predict"

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


@router.post("/predict/batch/")
async def upload_batch(file: UploadFile):
    content = await file.read()
    sha256 = hashlib.sha256(content).hexdigest()[:12]
    local_path = os.path.join(DATA_DIR, f"batch_{sha256}.csv")
    output_path = os.path.join(OUTPUT_DIR, f"batch_{sha256}")

    try:
        # 儲存 CSV 檔案
        with open(local_path, "wb") as f:
            f.write(content)

        # 執行批次預測
        run_batch_prediction(local_path, output_path)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"批次預測失敗: {e}")

    return {"status": "success", "batch_id": sha256}

@router.get("/predict/batch/{batch_id}")
def get_batch_result(batch_id: str):
    folder_path = os.path.join(OUTPUT_DIR, f"batch_{batch_id}")
    csv_path = os.path.join(OUTPUT_DIR, f"batch_{batch_id}.csv")

    # 還沒完成：預測資料夾不存在
    if not os.path.exists(folder_path):
        return JSONResponse(status_code=202, content={"status": "pending", "message": "預測尚未完成"})

    # 未轉換為 CSV 時，讀取 parquet 資料夾進行轉換
    if not os.path.exists(csv_path):
        try:
            df = spark.read.parquet(folder_path)  # 這裡讀的是整個資料夾
            df.limit(5000).toPandas().to_csv(csv_path, index=False)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"轉換 CSV 失敗: {e}")

    return FileResponse(path=csv_path, media_type='text/csv', filename=f"batch_{batch_id}.csv")

