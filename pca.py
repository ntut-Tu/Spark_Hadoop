import umap
import matplotlib.pyplot as plt
import numpy as np

from pyspark.sql import SparkSession
from preprocessing import transformers, mental_score, background_score, normalization

spark = SparkSession.builder.appName("PCA_UMAP").getOrCreate()

df = spark.read.parquet("/data/full/*.parquet")

df = transformers.apply_transformations(df)
df = mental_score.compute_mental_score(df)
df = background_score.compute_background_score(df)
df = normalization.apply_scaling(df)

scaled_selected = df.select("features", "score_cluster")
scaled_pd = scaled_selected.toPandas()

features_array = np.vstack(scaled_pd['features'])

umap_model = umap.UMAP(random_state=42)
umap_embedding = umap_model.fit_transform(features_array)

scaled_pd['umap1'] = umap_embedding[:,0]
scaled_pd['umap2'] = umap_embedding[:,1]

plt.figure(figsize=(8,6))
scatter = plt.scatter(
    scaled_pd['umap1'],
    scaled_pd['umap2'],
    c=scaled_pd['score_cluster'],
    cmap='tab10',
    s=50,
    alpha=0.8
)

plt.colorbar(scatter, label='Cluster Label')
plt.title("UMAP Projection (colored by Cluster)")
plt.xlabel("UMAP1")
plt.ylabel("UMAP2")

plt.savefig("umap_plot.png", dpi=300, bbox_inches='tight')
plt.close()
