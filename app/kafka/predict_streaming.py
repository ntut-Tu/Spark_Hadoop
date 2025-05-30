import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType

from configs.config_loader import load_config
from configs.enum_headers import RawColumns
from preprocessing.pipeline import run_prediction_pipeline, config

# === 初始化 log ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, '../configs/paths.yaml')
config = load_config(CONFIG_PATH, project_base=BASE_DIR, use_hdfs=True)
ts = datetime.now().strftime('%Y%m%d_%H%M%S')

logging.basicConfig(
    filename=f"kafka_predict_{ts}.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

console = logging.StreamHandler()
console.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(console)

# === Kafka 結構 schema ===
schema = StructType() \
    .add(RawColumns.Student_ID.value, StringType()) \
    .add(RawColumns.Gender.value, StringType()) \
    .add(RawColumns.Extracurricular_Activities.value, StringType()) \
    .add(RawColumns.Internet_Access_at_Home.value, StringType()) \
    .add(RawColumns.Family_Income_Level.value, StringType()) \
    .add(RawColumns.Parent_Education_Level.value, StringType()) \
    .add(RawColumns.Department.value, StringType()) \
    .add(RawColumns.Grade.value, StringType()) \
    .add(RawColumns.Study_Hours_per_Week.value, DoubleType()) \
    .add(RawColumns.Final_Score.value, DoubleType())

def safe_run_prediction(df, batch_id):
    logger.info(f"📦 接收到新的 batch_id={batch_id}，資料筆數: {df.count()}")
    try:
        run_prediction_pipeline(df, batch_id)
    except Exception as e:
        logger.error(f"❌ 執行預測時發生錯誤 (batch_id={batch_id})", exc_info=True)

# === 建立 Spark Streaming 任務 ===
try:
    spark = SparkSession.builder.appName("KafkaPredictStream").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("✅ Spark Session 建立成功")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "predict_topic") \
        .option("startingOffsets", "earliest") \
        .load()

    raw_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
    logger.info("🔍 Kafka 訂閱成功，準備解析 JSON schema")

    # debug log of raw json
    debug_query = raw_df.writeStream \
        .format("console") \
        .option("truncate", False) \
        .start()

    parsed_df = raw_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

    logger.info("🚀 Kafka streaming 開始執行預測任務...")

    main_query = parsed_df.writeStream \
        .foreachBatch(safe_run_prediction) \
        .start()

    logger.info(f"🎯 Streaming 啟動成功: isActive={main_query.isActive}")
    main_query.awaitTermination()

except Exception as e:
    logger.error("🔥 Kafka Spark Streaming 啟動失敗", exc_info=True)
