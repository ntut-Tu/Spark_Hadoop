# batch_predict.py
import os
from datetime import datetime
from pyspark.sql import SparkSession
from utils import load_data
from configs.config_loader import load_config
from configs.enum_headers import CandidateColumns
from preprocessing import normalization
from preprocessing.transform import transformers
from preprocessing.label_mapper import label_mapping
from preprocessing.scoring import background_score, mental_score
from clustering import score_cluster, background_cluster
from utils.column_utils import convert_boolean_to_int


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'configs/paths.yaml')
config = load_config(CONFIG_PATH, project_base=BASE_DIR, use_hdfs=True)

spark = SparkSession.builder.appName("BatchPredict").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def run_batch_prediction(hdfs_input_path: str, output_base: str):
    df = load_data.read_raw_data(spark, hdfs_input_path)

    df = transformers.apply_raw_column_renaming(df)
    origin_data = df.select("*")
    df = transformers.apply_to_candidate_transformations(df)
    df = normalization.apply_scaling(df)
    df = convert_boolean_to_int(df)
    df = mental_score.compute_mental_score(df)
    df = background_score.compute_background_score(df)

    df = score_cluster.predict_with_score_model(df, config)
    df1 = df.select(CandidateColumns.student_id, "score_cluster")
    df = background_cluster.predict_with_background_model(df, config)
    df2 = df.select(CandidateColumns.student_id, "background_cluster")

    full_output = origin_data.join(df1, on=CandidateColumns.student_id, how="inner")
    full_output = full_output.join(df2, on=CandidateColumns.student_id, how="inner")
    full_output = label_mapping(full_output)

    full_output.write.mode("overwrite").parquet(output_base)
    return output_base
