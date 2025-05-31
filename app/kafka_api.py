from fastapi import FastAPI, HTTPException, APIRouter

from configs.schemas import PredictRequest
from kafkas.kafka_client import KafkaClient

client = KafkaClient()
router = APIRouter()


@router.post("/predict/")
def predict(payload: PredictRequest):
    try:
        client.send_input("predict_topic", payload.dict())
        return {
            "status": "sent",
            "student_id": payload.Student_ID,
            "message": "prediction request sent"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka error: {e}")


@router.get("/predict/{student_id}")
def get_result(student_id: str):
    try:
        if client.has_result(student_id):
            result = client.get_result_nowait(student_id)
            return {
                "status": "done",
                "result": result
            }
        return {"status": "pending"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka error: {e}")