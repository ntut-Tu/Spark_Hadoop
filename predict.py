import os

from pyspark.sql import SparkSession

from clustering import score_cluster, background_cluster
from config.config_loader import load_config
from preprocessing import load_data, transformers, mental_score, background_score, normalization
from preprocessing.label_mapper import label_mapping

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config/paths.yaml')
config = load_config(CONFIG_PATH, project_base=BASE_DIR, use_hdfs=True)

spark = SparkSession.builder.appName("StudentPreprocessing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load data
df = load_data.read_raw_data(spark, config['data']['predict'])
origin_data = df.select("*")

df = transformers.apply_transformations(df)
df = mental_score.compute_mental_score(df)
df = background_score.compute_background_score(df)
df = normalization.apply_scaling(df)

df = score_cluster.predict_with_score_model(df, config)
df1 = df.select("student_id", "score_cluster")
df = background_cluster.predict_with_background_model(df, config)
df2 = df.select("student_id","background_cluster")

full_output = origin_data.join(df1,on="student_id",how="inner")
full_output = full_output.join(df2,on="student_id",how="inner")
full_output = label_mapping(full_output)
full_output.write.mode("overwrite").parquet(config['data']['predict_output'])