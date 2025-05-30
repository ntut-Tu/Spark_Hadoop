from typing import List, Dict

from fastapi import APIRouter, FastAPI, File, UploadFile, HTTPException
from service import job_service, file_service

router = APIRouter()
app = FastAPI()

default_model = "default"
default_predict_path = "data/raw/predict.csv"

@app.post("/upload/")
async def upload_file(file: UploadFile = File(...)):
    try:
        content = await file.read()
        return file_service.save_file(file.filename, content)
    except Exception as e:
        raise HTTPException(status_code=400,detail=f"error {e}")

@router.post("/v1/batch_submit")
def bulk_submit_spark_job_v1(path: str = default_predict_path):
    """
    Bulk submit a Spark job (predict job), start a new cluster are not supported in v1.
    """
    try:
        return job_service.submit_job(path)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"error {e}")

class StreamSubmitPayload:
    """
    Payload for stream input job submission.
    """
    model_id: str = default_model
    prediction_inputs: List[Dict] = []  # List of maps (key-value)

@router.post("/v1/stream_submit")
def submit_spark_job_v1(payload: StreamSubmitPayload):
    """
    Submit a Spark job with stream input.
    Although we have used the list[map] structure, due to the difficulty of validating the input format v1 currently only provides a single input.
    """
    try:
        return job_service.submit_job_with_stream(payload)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"error {e}")

@router.get("/v1/models")
def list_models_v1():
    """
    List all Spark models.
    """
    # v1 will only return the default model, but this interface is designed to be extensible
    try:
        return job_service.list_models()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"error {e}")

@router.get("/v1/models/{model_id}")
def get_model_detail_v1(model_id: str = default_model):
    """
    Get a specific Spark model info.
    """
    try:
        return job_service.get_model_detail(model_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"error {e}")

@router.get("/v1/jobs")
def list_spark_jobs_v1(model_id: str = default_model):
    """
    List all Spark jobs for a given model.
    """
    try:
        return job_service.list_jobs(model_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"error {e}")

@router.get("/v1/status/{job_id}")
def get_spark_job_status_v1(job_id: str):
    """
    Get the status of a Spark job.
    """
    try:
        return job_service.get_job_status(job_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"error {e}")

@router.get("/v1/result/{job_id}")
def get_spark_job_result_v1(job_id: str):
    """
    Get the result of a Spark job.
    """
    try:
        return job_service.get_job_result(job_id)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"error {e}")
