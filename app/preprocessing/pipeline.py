import os
import logging
from datetime import datetime
from clustering import background_cluster, score_cluster, mental_cluster
from configs.config_loader import load_config
from configs.enum_headers import CandidateColumns
from preprocessing import normalization
from preprocessing.transform import transformers
from preprocessing.scoring import background_score, mental_score
from preprocessing.label_mapper import label_mapping
from utils.column_utils import convert_boolean_to_int, ensure_all_raw_columns
from utils.load_data import get_unique_output_path
from pyspark.sql.functions import col


# === Set config & timestamp ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, '../configs/paths.yaml')
config = load_config(CONFIG_PATH, project_base=BASE_DIR, use_hdfs=True)
ts = datetime.now().strftime('%Y%m%d_%H%M%S')

# === Configure logger ===
logging.basicConfig(
    filename=f"./logs/pipeline_{ts}.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_prediction_pipeline(df, batch_id=None):
    logger.info("🚀 開始執行預測流程")
    try:
        df = ensure_all_raw_columns(df)
        df = transformers.apply_raw_column_renaming(df)
        origin_df = df
        df = transformers.apply_to_candidate_transformations(df)
        null_counts = df.select([
            (col(c).isNull().cast("int")).alias(c) for c in df.columns
        ]).groupBy().sum().collect()[0].asDict()

        for col_name, null_count in null_counts.items():
            if null_count > 0:
                logger.warning(f"🔍 欄位 {col_name} 有 {null_count} 筆 null 值")


        rows = df.collect()
        for row in rows:
            logger.warning(f"full data: {row.asDict()}")

        df = normalization.apply_scaling(df)
        df = convert_boolean_to_int(df)
        df = mental_score.compute_mental_score(df)
        df = background_score.compute_background_score(df)

        df = score_cluster.predict_with_score_model(df, config)
        df1 = df.select(CandidateColumns.student_id, "score_cluster")
        df = background_cluster.predict_with_background_model(df, config)
        df2 = df.select(CandidateColumns.student_id, "background_cluster")
        df = mental_cluster.predict_with_mental_model(df, config)
        df3 = df.select(CandidateColumns.student_id, "mental_cluster")

        full_output = origin_df.join(df1, on=CandidateColumns.student_id, how="inner")
        full_output = full_output.join(df2, on=CandidateColumns.student_id, how="inner")
        full_output = full_output.join(df3, on=CandidateColumns.student_id, how="inner")
        full_output = label_mapping(full_output)

        full_output.select(
            CandidateColumns.student_id, "score_cluster", "background_cluster", "mental_cluster",
        ).show(truncate=False)

        output_path = get_unique_output_path(config['data']['predict_output'])
        logger.info(f"💾 儲存結果至 parquet: {output_path}")
        full_output.write.parquet(output_path)

        kafka_output = full_output.selectExpr("to_json(struct(*)) AS value")
        kafka_output.write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "predict_result_topic") \
            .save()

        logger.info("✅ 預測流程完成")
    except Exception as e:
        logger.error("❌ 預測流程錯誤", exc_info=True)
