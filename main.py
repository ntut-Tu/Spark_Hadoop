# main.py
import yaml
from pyspark.sql import SparkSession
from preprocessing import load_data, transformers, normalization, mental_score
from clustering import score_cluster, background_cluster, cluster_analysis

with open("config/paths.yaml", "r") as f:
    config = yaml.safe_load(f)

spark = SparkSession.builder.appName("StudentPreprocessing").getOrCreate()

# Load data
df = load_data.read_raw_data(spark, config['data']['raw'])

# Preprocessing
df = transformers.apply_transformations(df)
df = mental_score.compute_mental_score(df)
df = normalization.apply_scaling(df)

# Save interim data
df.write.mode("overwrite").parquet(config['data']['interim'])

# Clustering
score_cluster.run(df, config)
background_cluster.run(df, config)
cluster_analysis.cross_tab(df, config)

# Save processed data
df.write.mode("overwrite").parquet(config['data']['processed'])