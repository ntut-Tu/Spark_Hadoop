import json
import time
from kafka import KafkaProducer, KafkaConsumer
from pyspark.sql import SparkSession
from configs.enum_headers import RawColumns

def test_end_to_end_prediction():
    # 1️⃣ 準備測試資料
    test_record = {
        RawColumns.Student_ID.value: "E2E_123",
        RawColumns.Gender.value: "Male",
        RawColumns.Extracurricular_Activities.value: "Yes",
        RawColumns.Internet_Access_at_Home.value: "Yes",
        RawColumns.Family_Income_Level.value: "Medium",
        RawColumns.Parent_Education_Level.value: "Bachelor's",
        RawColumns.Department.value: "CS",
        RawColumns.Grade.value: "A",
        RawColumns.Study_Hours_per_Week.value: 15.0,
        RawColumns.Final_Score.value: 90.0
    }

    # 2️⃣ Kafka Producer 發送
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    producer.send("predict_topic", test_record)
    producer.flush()
    print("✅ 測試資料已送出")

    # 3️⃣ Kafka Consumer 等待結果
    consumer = KafkaConsumer(
        'predict_result_topic',
        bootstrap_servers='kafka:9092',
        group_id='e2e-test-group',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print("🎧 等待結果中...")
    start_time = time.time()
    timeout = 20

    while time.time() - start_time < timeout:
        for message in consumer:
            result = message.value
            if result.get(RawColumns.Student_ID.value) == "E2E_123":
                print("🎯 接收到預測結果：", result)
                assert "score_cluster" in result
                assert "background_cluster" in result
                return

    raise TimeoutError("❌ 超過 20 秒未接收到預測結果")
