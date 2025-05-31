# api_server.py
import hashlib
import os
import shutil
from fastapi import FastAPI, UploadFile, HTTPException, APIRouter
from pyspark.sql import SparkSession

from run_batch_prediction import run_batch_prediction

router = APIRouter()
spark = SparkSession.builder.appName("BatchPredictAPI").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


@router.post("/predict/batch/")
async def upload_batch(file: UploadFile):
    content = await file.read()
    sha256 = hashlib.sha256(content).hexdigest()[:12]
    local_path = f"/tmp/batch_{sha256}.csv"
    hdfs_path = f"/data/predict/batch_{sha256}.csv"
    hdfs_output_path = f"/data/processed/predict/batch_{sha256}"

    with open(local_path, "wb") as f:
        f.write(content)

    os.system(f"docker exec namenode hdfs dfs -put -f {local_path} {hdfs_path}")

    try:
        run_batch_prediction(hdfs_path, hdfs_output_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"批次預測失敗: {e}")

    return {"status": "success", "batch_id": sha256}


@router.get("/predict/batch/{batch_id}")
def get_batch_result(batch_id: str):
    parquet_path = f"/data/processed/predict/batch_{batch_id}.parquet"
    try:
        df = spark.read.parquet(parquet_path)
        data = [row.asDict() for row in df.limit(1000).collect()]
        return {"status": "success", "result": data}
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"找不到結果或讀取失敗: {e}")
