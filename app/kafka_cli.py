from configs.enum_headers import RawColumns
from kafkas.kafka_client import KafkaClient


class KafkaCLI:
    def __init__(self):
        self.client = KafkaClient()
        self.client.start_consumer(
            topic='predict_result_topic',
            group_id='cli-consumer',
            on_message=self._on_result
        )

    def _on_result(self, value: dict):
        print(f"\n🎯 預測完成：{value}\n👉 請繼續輸入資料")

    def run(self):
        print("📤 輸入學生原始資料（空行離開）：")

        while True:
            try:
                mode = input("請選擇模式 (modeA/modeB)：").strip().lower()
                if not mode:
                    break
                if mode not in ["modea", "modeb"]:
                    print("⚠️ 無效模式，請輸入 modeA 或 modeB")
                    continue

                if mode == "modeb":
                    student_id = input("Student ID: ").strip()
                    if not student_id:
                        break
                    record = {
                        RawColumns.Student_ID.value: student_id,
                        RawColumns.Gender.value: input("Gender (Male/Female): ").strip(),
                        RawColumns.Extracurricular_Activities.value: input("Extracurricular_Activities (Yes/No): ").strip(),
                        RawColumns.Internet_Access_at_Home.value: input("Internet_Access_at_Home (Yes/No): ").strip(),
                        RawColumns.Family_Income_Level.value: input("Family_Income_Level (High/Medium/Low): ").strip(),
                        RawColumns.Parent_Education_Level.value: input("Parent_Education_Level (None/High School/Bachelor's/Master's/PhD): ").strip(),
                        RawColumns.Department.value: input("Department (Mathematics/Business/Engineering/CS): ").strip(),
                        RawColumns.Grade.value: input("Grade (A/B/C/D/F): ").strip(),
                        RawColumns.Study_Hours_per_Week.value: float(input("Study hours per week: ").strip()),
                        RawColumns.Final_Score.value: float(input("Final score: ").strip())
                    }
                else:  # modeA
                    record = {
                        RawColumns.Sleep_Hours_per_Night: float(input("Sleep hours per night: ").strip()),
                        RawColumns.Attendance_Percent: float(input("Attendance (%) e.g. 90: ").strip()),
                        RawColumns.Stress_Level: float(input("Stress Level (1~10): ").strip()),
                        RawColumns.Extracurricular_Activities.value: input("Extracurricular_Activities (Yes/No): ").strip(),
                        RawColumns.Internet_Access_at_Home.value: input("Internet_Access_at_Home (Yes/No): ").strip(),
                    }

                self.client.send_input("predict_topic", record)
                print("✅ 已送出")

            except KeyboardInterrupt:
                print("\n⛔ 中斷輸入")
                break
            except Exception as e:
                print(f"❌ 發生錯誤: {e}")


if __name__ == "__main__":
    cli = KafkaCLI()
    cli.run()
