import os
import yaml
from pyspark.sql import SparkSession
from config.config_loader import load_config
from preprocessing import load_data, transformers, normalization, mental_score
from clustering import score_cluster, background_cluster, cluster_analysis

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, 'config/paths.yaml')
config = load_config(CONFIG_PATH, project_base=BASE_DIR, use_hdfs=True)
print("📦 interim 路徑:", config['data']['interim'])
print("📦 processed 路徑:", config['data']['processed'])

spark = SparkSession.builder.appName("StudentPreprocessing").getOrCreate()

# Load data
df = load_data.read_raw_data(spark, config['data']['raw'])

# Preprocessing
df = transformers.apply_transformations(df)
df = mental_score.compute_mental_score(df)
df = normalization.apply_scaling(df)

# Save interim data
print("✅ 正在寫入 interim 資料至 HDFS...")
df.write.mode("overwrite").parquet(config['data']['interim'])

# Clustering
df = score_cluster.run(df, config)
df = background_cluster.run(df, config)
cluster_analysis.cross_tab(df, config)

# Save processed data
print("✅ 正在寫入 processed 資料至 HDFS...")
df.write.mode("overwrite").parquet(config['data']['processed'])