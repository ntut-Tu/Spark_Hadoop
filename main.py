import os
from pyspark.sql import SparkSession
from config.config_loader import load_config
from config.enum_headers import CandidateColumns
from preprocessing import normalization
from preprocessing.transform import transformers
from utils import load_data
from preprocessing.scoring import background_score, mental_score
from clustering import score_cluster, background_cluster, cluster_analysis
from preprocessing.label_mapper import label_mapping
from utils.column_utils import clean_column_names, convert_boolean_to_int

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config/paths.yaml')
config = load_config(CONFIG_PATH, project_base=BASE_DIR, use_hdfs=True)
print("📦 interim 路徑:", config['data']['interim'])
print("📦 processed 路徑:", config['data']['processed'])

spark = SparkSession.builder.appName("StudentPreprocessing").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Load data
df = load_data.read_raw_data(spark, config['data']['raw'])

# Preprocessing
df = transformers.apply_raw_column_renaming(df)
df.show(5, truncate=False)
origin_data = df.select("*")
df = transformers.apply_to_candidate_transformations(df)
df.show(5, truncate=False)
df = normalization.apply_scaling(df)
df = convert_boolean_to_int(df)
df = mental_score.compute_mental_score(df)
df = background_score.compute_background_score(df)

# Save interim data
print("✅ 正在寫入 interim 資料至 HDFS...")
df.write.mode("overwrite").parquet(config['data']['interim'])

# Clustering
df = score_cluster.run(df, config)
df1 = df.select(CandidateColumns.student_id, "score_cluster")
df = background_cluster.run(df, config)
df2 = df.select(CandidateColumns.student_id, "background_cluster")
cluster_analysis.cross_tab(df1, df2, config)

# Save processed data
print("✅ 正在寫入 processed 資料至 HDFS...")
#df.write.mode("overwrite").parquet(config['data']['processed'])
full_output = origin_data.join(df1,on=CandidateColumns.student_id,how="inner")
full_output = full_output.join(df2,on=CandidateColumns.student_id,how="inner")
full_output = label_mapping(full_output)
full_output.write.mode("overwrite").parquet(config['data']['full'])