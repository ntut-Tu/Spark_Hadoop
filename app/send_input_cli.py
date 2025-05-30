from kafka import KafkaProducer, KafkaConsumer
import threading, json
from configs.enum_headers import RawColumns

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',  # docker-compose service 名稱
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("📤 輸入學生原始資料（空行離開）：")

# Kafka Consumer（獨立執行緒）
def listen_for_results():
    consumer = KafkaConsumer(
        'predict_result_topic',
        bootstrap_servers='kafka:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='cli-consumer'
    )
    print("🎧 等待預測結果...")
    for msg in consumer:
        print(f"\n🎯 預測完成：{msg.value}\n👉 請繼續輸入資料")

threading.Thread(target=listen_for_results, daemon=True).start()

while True:
    try:
        student_id = input("student_id: ").strip()
        if not student_id:
            break

        record = {
            RawColumns.Student_ID.value: student_id,
            RawColumns.Gender.value: input("Gender (Male/Female): "),
            RawColumns.Extracurricular_Activities.value: input("Extracurricular_Activities (Yes/No): "),
            RawColumns.Internet_Access_at_Home.value: input("Internet_Access_at_Home (Yes/No): "),
            RawColumns.Family_Income_Level.value: input("Family_Income_Level (High/Medium/Low): "),
            RawColumns.Parent_Education_Level.value: input("Parent_Education_Level (None/High School/Bachelor's/Master's/PhD): "),
            RawColumns.Department.value: input("Department (Mathematics/Business/Engineering/CS): "),
            RawColumns.Grade.value: input("Grade (A/B/C/D/F): "),
            RawColumns.Study_Hours_per_Week.value: float(input("Study hours per week: ")),
            RawColumns.Final_Score.value: float(input("Final score: "))
        }

        producer.send("predict_topic", record)
        producer.flush()
        print("✅ 已送出")
    except KeyboardInterrupt:
        break
    except Exception as e:
        print(f"❌ 發生錯誤: {e}")

