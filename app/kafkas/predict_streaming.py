import json
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StringType, DoubleType

from configs.config_loader import load_config
from configs.enum_headers import RawColumns
from preprocessing.pipeline import run_prediction_pipeline


class KafkaPredictor:
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.config_path = os.path.join(self.base_dir, '../configs/paths.yaml')
        self.config = load_config(self.config_path, project_base=self.base_dir, use_hdfs=True)
        self.spark = self._create_spark_session()
        self.schema = self._build_schema()

    def _create_spark_session(self):
        spark = SparkSession.builder.appName("KafkaPredictStream").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        return spark

    def _build_schema(self):
        return StructType() \
            .add(RawColumns.Student_ID.value, StringType()) \
            .add(RawColumns.Gender.value, StringType()) \
            .add(RawColumns.Extracurricular_Activities.value, StringType()) \
            .add(RawColumns.Internet_Access_at_Home.value, StringType()) \
            .add(RawColumns.Family_Income_Level.value, StringType()) \
            .add(RawColumns.Parent_Education_Level.value, StringType()) \
            .add(RawColumns.Department.value, StringType()) \
            .add(RawColumns.Grade.value, StringType()) \
            .add(RawColumns.Study_Hours_per_Week.value, DoubleType()) \
            .add(RawColumns.Total_Score.value, DoubleType())\
            .add(RawColumns.Projects_Score, DoubleType()) \
            .add(RawColumns.Quizzes_Avg, DoubleType())\
            .add(RawColumns.Sleep_Hours_per_Night, DoubleType()) \
            .add(RawColumns.Attendance_Percent, DoubleType()) \
            .add(RawColumns.Stress_Level, DoubleType()) \


    def _safe_run_prediction(self, df, batch_id):
        try:
            run_prediction_pipeline(df, batch_id)
        except Exception as e:
            print(f"[Error] prediction failed: {e}")

    def start(self):
        kafka_df = self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", "predict_topic") \
            .option("startingOffsets", "earliest") \
            .load()

        raw_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

        # raw_df.writeStream.format("console").option("truncate", False).start()

        parsed_df = raw_df.select(from_json(col("json_str"), self.schema).alias("data")).select("data.*")

        query = parsed_df.writeStream.foreachBatch(self._safe_run_prediction).start()
        query.awaitTermination()


if __name__ == "__main__":
    try:
        predictor = KafkaPredictor()
        predictor.start()
    except Exception as e:
        print(f"[Fatal Error] {e}")
