from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import batch_api
import kafka_api

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup_event():
    def on_kafka_message(data):
        print(f"[Kafka] 收到訊息: {data}")
    kafka_api.client.start_consumer("predict_result_topic", "api-group", on_kafka_message)
app.include_router(kafka_api.router)
app.include_router(batch_api.router)
