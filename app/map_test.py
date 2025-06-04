import umap.umap_ as umap
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.cm as cm
from pyspark.sql import SparkSession

from configs.enum_headers import CandidateColumns
from configs.feature_select import get_feature_list_for_clustering
from preprocessing import normalization
from preprocessing.transform import transformers
from preprocessing.scoring import background_score, mental_score
from utils.column_utils import convert_boolean_to_int

# 建立 Spark Session
spark = SparkSession.builder.appName("PCA_UMAP").getOrCreate()

# 讀取資料
df = spark.read.parquet("hdfs://namenode:9000/data/full/")

# 前處理
df = transformers.apply_raw_column_renaming(df)
df = transformers.apply_to_candidate_transformations(df)
df = normalization.apply_scaling(df)
df = convert_boolean_to_int(df)
df = mental_score.compute_mental_score(df)
df = background_score.compute_background_score(df)

# 特徵欄位（不包含 student_id）
feature_cols = [col for col in get_feature_list_for_clustering() if col != CandidateColumns.student_id]

# 取出需要欄位
scaled_selected = df.select(*feature_cols, "score_cluster", "background_cluster")
scaled_pd = scaled_selected.toPandas()
features_array = scaled_pd[feature_cols].values

# 執行 UMAP
umap_model = umap.UMAP(random_state=42, n_neighbors=20, min_dist=0.5)
umap_embedding = umap_model.fit_transform(features_array)
scaled_pd['umap1'] = umap_embedding[:, 0]
scaled_pd['umap2'] = umap_embedding[:, 1]

# 分群 label
score_labels = np.unique(scaled_pd['score_cluster'])
background_labels = np.unique(scaled_pd['background_cluster'])

# 自動配色 colormap
max_k = max(len(score_labels), len(background_labels))
cmap_dynamic = cm.get_cmap('tab10', max_k)

# 畫圖
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Score Cluster 圖
scatter1 = axes[0].scatter(
    scaled_pd['umap1'],
    scaled_pd['umap2'],
    c=scaled_pd['score_cluster'],
    cmap=cmap_dynamic,
    s=50,
    alpha=0.8
)
axes[0].set_title("UMAP colored by Score Cluster")
axes[0].set_xlabel("UMAP1")
axes[0].set_ylabel("UMAP2")
cbar1 = plt.colorbar(scatter1, ax=axes[0], ticks=score_labels)
cbar1.set_label("Score Cluster")

# Background Cluster 圖
scatter2 = axes[1].scatter(
    scaled_pd['umap1'],
    scaled_pd['umap2'],
    c=scaled_pd['background_cluster'],
    cmap=cmap_dynamic,
    s=50,
    alpha=0.8
)
axes[1].set_title("UMAP colored by Background Cluster")
axes[1].set_xlabel("UMAP1")
axes[1].set_ylabel("UMAP2")
cbar2 = plt.colorbar(scatter2, ax=axes[1], ticks=background_labels)
cbar2.set_label("Background Cluster")

# 輸出
plt.tight_layout()
plt.savefig("umap_dual_view_NEW.png", dpi=300)
plt.close()
