import umap.umap_ as umap
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.colors as mcolors

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

# 資料前處理流程
df = transformers.apply_raw_column_renaming(df)
df = transformers.apply_to_candidate_transformations(df)
df = normalization.apply_scaling(df)
df = convert_boolean_to_int(df)
df = mental_score.compute_mental_score(df)
df = background_score.compute_background_score(df)

# 選擇特徵欄位（排除 ID）
feature_cols = [col for col in get_feature_list_for_clustering() if col != CandidateColumns.student_id]

# 提取特徵與分群欄位
scaled_selected = df.select(*feature_cols, "score_cluster", "mental_cluster")

# 轉 Pandas + NumPy
scaled_pd = scaled_selected.toPandas()
features_array = scaled_pd[feature_cols].values

# 執行 UMAP
umap_model = umap.UMAP(random_state=42, n_neighbors=20, min_dist=0.5)
umap_embedding = umap_model.fit_transform(features_array)
cmap3 = mcolors.ListedColormap(['#1f77b4', '#ff7f0e', '#2ca02c'])

# 加入 UMAP 結果
scaled_pd['umap1'] = umap_embedding[:, 0]
scaled_pd['umap2'] = umap_embedding[:, 1]

# ✨ 畫雙圖比較
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# 左邊畫 score_cluster
scatter1 = axes[0].scatter(
    scaled_pd['umap1'],
    scaled_pd['umap2'],
    c=scaled_pd['score_cluster'],
    cmap=cmap3,
    s=50,
    alpha=0.8
)
axes[0].set_title("UMAP colored by Score Cluster")
axes[0].set_xlabel("UMAP1")
axes[0].set_ylabel("UMAP2")
cbar1 = plt.colorbar(scatter1, ax=axes[0], label='Score Cluster',ticks = [0, 1, 2])

# 右邊畫 background_cluster
scatter2 = axes[1].scatter(
    scaled_pd['umap1'],
    scaled_pd['umap2'],
    c=scaled_pd['mental_cluster'],
    cmap=cmap3,
    s=50,
    alpha=0.8
)
axes[1].set_title("UMAP colored by Mental Cluster")
axes[1].set_xlabel("UMAP1")
axes[1].set_ylabel("UMAP2")
cbar2 = plt.colorbar(scatter2, ax=axes[1], label='Metal Score',ticks = [0, 1, 2])

# 儲存圖檔
plt.tight_layout()
plt.savefig("umap_dual_view(mental).png", dpi=300)
plt.close()
