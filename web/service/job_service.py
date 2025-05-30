import csv
import io
import os
import subprocess

from router.job_v1_router import StreamSubmitPayload
from scripts import spark_job_builder
from service import file_service

PREDICTION_DIR = "/data/prediction"

def submit_job(path: str):
    if not os.path.exists(path):
        raise ValueError("Path not found")

    result = spark_job_builder.build_and_submit(path)

    return result

def submit_job_with_stream(payload: StreamSubmitPayload):
    # check if the input length is 1,else raise an error
    if len(payload.prediction_inputs) != 1:
        raise ValueError("Only one input is allowed in v1")

    input_data = payload.prediction_inputs[0]

    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=input_data.keys())
    writer.writeheader()
    writer.writerow(input_data)

    content = buffer.getvalue().encode("utf-8")

    # write the stream to a temporary CSV file
    path = file_service.save_file("stream_input.csv", content)

    return submit_job(path)

def list_models():
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-ls", "/model"],
            capture_output=True,
            text=True,
            check=True
        )

        lines = result.stdout.strip().split("\n")[1:]  # 忽略第一行 summary
        files = [line.split()[-1] for line in lines]   # 每行最後一欄是檔案路徑

        return {"models": files}
    except subprocess.CalledProcessError as e:
        return {"error": f"HDFS 錯誤: {e.stderr.strip()}"}
    except Exception as e:
        return {"error": str(e)}

def get_model_detail(model_id):
    # TODO: model information should be generated when the model is clustered
    return None

def list_jobs(model_id):
    # TODO: prediction's info should be generated when the prediction is made
    # For now we list all jobs
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-ls", PREDICTION_DIR],
            capture_output=True,
            text=True,
            check=True
        )

        lines = result.stdout.strip().split("\n")[1:]  # 跳過 header
        files = [line.split()[-1] for line in lines]

        return {
            "model_id": model_id,
            "status": "ready",
            "prediction_files": files
        }

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"HDFS 錯誤: {e.stderr.strip()}")


def get_job_status(job_id):
    return None


def get_job_result(job_id):
    return None


