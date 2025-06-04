import time
import pytest
from configs.enum_headers import RawColumns
from app.kafkas.kafka_client import KafkaClient


class KafkaE2ETester:
    def __init__(self, input_topic="predict_topic", result_topic="predict_result_topic"):
        self.client = KafkaClient()
        self.input_topic = input_topic
        self.result_topic = result_topic

    def send_test_record(self, record: dict):
        self.client.send_input(self.input_topic, record)

    def wait_for_result(self, student_id: str, timeout: int = 20):
        consumer = self.client._create_consumer(
            topic=self.result_topic,
            group_id="e2e-test-group",
            auto_offset_reset="earliest"
        )

        start_time = time.time()
        for message in consumer:
            result = message.value
            if result.get(RawColumns.Student_ID.value, "") == student_id:
                return result
            if time.time() - start_time > timeout:
                break

        raise TimeoutError(f"❌ 超過 {timeout} 秒未接收到 Student_ID={student_id} 的預測結果")


@pytest.mark.parametrize("mode", ["modeA", "modeB"])
def test_end_to_end_prediction(mode):
    tester = KafkaE2ETester()

    if mode == "modeB":
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
            RawColumns.Total_Score.value: 90.0
        }
        tester.send_test_record(test_record)
        result = tester.wait_for_result("E2E_123", timeout=20)
        assert "score_cluster" in result
        assert "background_cluster" in result
        assert result[RawColumns.Student_ID.value] == "E2E_123"

    else:  # modeA
        test_record = {
            RawColumns.Sleep_Hours_per_Night: 8.0,
            RawColumns.Attendance_Percent: 92.0,
            RawColumns.Stress_Level: 5.0,
            RawColumns.Extracurricular_Activities.value: "No",
            RawColumns.Internet_Access_at_Home.value: "Yes",
            RawColumns.Total_Score.value: 90.0
        }
        tester.send_test_record(test_record)
        result = tester.wait_for_result(student_id="", timeout=20)  # 沒有 Student_ID
        assert "score_cluster" in result
        assert "mental_cluster" in result
