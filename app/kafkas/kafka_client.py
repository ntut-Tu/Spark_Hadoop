import json
import queue

from kafka import KafkaProducer, KafkaConsumer
import threading
from typing import Callable, Optional


class KafkaClient:
    def __init__(self, bootstrap_servers='kafka:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.bootstrap_servers = bootstrap_servers
        self.result_store = {}

    def send_input(self, topic: str, record: dict):
        self.producer.send(topic, record)
        self.producer.flush()

    def start_consumer(
        self,
        topic: str,
        group_id: str,
        on_message: Callable[[dict], None],
        daemon: bool = True
    ):
        def _listen():
            print("[KafkaClient] 開始 listen Kafka 訊息...")
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=self.bootstrap_servers,
                value_deserializer=lambda v: json.loads(v.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id=group_id
            )
            for msg in consumer:
                data = msg.value
                student_id = data.get("Student_ID")
                if student_id not in self.result_store:
                    self.result_store[student_id] = queue.Queue()
                self.result_store[student_id].put(data)
                if on_message:
                    on_message(data)

        threading.Thread(target=_listen, daemon=daemon).start()

    def _create_consumer(self, topic: str, group_id: str, auto_offset_reset="latest"):
        return KafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

    def wait_for_result(self, student_id: str, timeout: int = 10) -> Optional[dict]:
        q = self.result_store.get(student_id)
        if not q:
            return None
        try:
            return q.get(timeout=timeout)
        except:
            return None

    def has_result(self, student_id: str) -> bool:
        print(f"[KafkaClient] {self.result_store}")
        return student_id in self.result_store and not self.result_store[student_id].empty()

    def get_result_nowait(self, student_id: str):
        if self.has_result(student_id):
            return self.result_store[student_id].get_nowait()
        return None
