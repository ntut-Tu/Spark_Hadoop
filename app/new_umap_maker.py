import os

import umap.umap_ as umap
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.cm as cm
from pyspark.sql import SparkSession

from configs.enum_headers import CandidateColumns
from configs.feature_select import get_feature_list_for_clustering, get_testing_score_features, get_score_features, \
    get_mental_features_for_scoring, get_background_features_for_scoring
from preprocessing import normalization
from preprocessing.transform import transformers
from preprocessing.scoring import background_score, mental_score
from utils.column_utils import convert_boolean_to_int
from utils.logger import short_id


def _target_selector(target, for_what):
    if target == "mental":
        if for_what == "cluster":
            return "mental_cluster"
        elif for_what == "feature":
            return get_mental_features_for_scoring()
    elif target == "background":
        if for_what == "cluster":
            return "background_cluster"
        elif for_what == "feature":
            return get_background_features_for_scoring()
    elif target == "score":
        if for_what == "cluster":
            return "score_cluster"
        elif for_what == "feature":
            return _get_score_features()
    else:
        raise ValueError(f"Unknown target: {target}")

def _get_score_features():
    input_type = "default"
    if input_type == "default":
        return get_score_features()
    elif input_type == "test":
        return get_testing_score_features()


def _clustering_feature_getter(target1, target2):
    if target2 == "mental":
        if target1 == "background":
            return get_mental_features_for_scoring() + get_background_features_for_scoring()
        elif target1 == "score":
            return _get_score_features() + get_mental_features_for_scoring()
    elif target2 == "background":
        if target1 == "mental":
            return get_mental_features_for_scoring() + get_background_features_for_scoring()
        elif target1 == "score":
            return get_background_features_for_scoring() + _get_score_features()
    elif target2 == "score":
        if target1 == "mental":
            return _get_score_features() + get_mental_features_for_scoring()
        elif target1 == "background":
            return _get_score_features() + get_background_features_for_scoring()


def _data_frame_fetcher():
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
    return df


# 建立 Spark Session
def create_umap(target1, target2, version):
    df = _data_frame_fetcher()

    # 特徵欄位（不包含 student_id）
    feature_cols = [col for col in _clustering_feature_getter(target1, target2) if col != CandidateColumns.student_id]

    # 取出需要欄位
    scaled_selected = df.select(*feature_cols, _target_selector(target1, "cluster"),
                                _target_selector(target2, "cluster"))
    scaled_pd = scaled_selected.toPandas()
    features_array = scaled_pd[feature_cols].values

    # 執行 UMAP
    umap_model = umap.UMAP(random_state=42, n_neighbors=20, min_dist=0.5)
    umap_embedding = umap_model.fit_transform(features_array)
    scaled_pd['umap1'] = umap_embedding[:, 0]
    scaled_pd['umap2'] = umap_embedding[:, 1]

    # 分群 label
    score_labels = np.unique(scaled_pd[_target_selector(target1, "cluster")])
    background_labels = np.unique(scaled_pd[_target_selector(target2, "cluster")])

    # 自動配色 colormap
    max_k = max(len(score_labels), len(background_labels))
    cmap_dynamic = cm.get_cmap('tab10', max_k)

    # 畫圖
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Score Cluster 圖
    scatter1 = axes[0].scatter(
        scaled_pd['umap1'],
        scaled_pd['umap2'],
        c=scaled_pd[_target_selector(target1, "cluster")],
        cmap=cmap_dynamic,
        s=50,
        alpha=0.8
    )
    axes[0].set_title(f"UMAP colored by {target1} Cluster")
    axes[0].set_xlabel("UMAP1")
    axes[0].set_ylabel("UMAP2")
    cbar1 = plt.colorbar(scatter1, ax=axes[0], ticks=score_labels)
    cbar1.set_label(f"{target1} Cluster")

    # Background Cluster 圖
    scatter2 = axes[1].scatter(
        scaled_pd['umap1'],
        scaled_pd['umap2'],
        c=scaled_pd[_target_selector(target2, "cluster")],
        cmap=cmap_dynamic,
        s=50,
        alpha=0.8
    )
    axes[1].set_title(f"UMAP colored by {target2} Cluster")
    axes[1].set_xlabel("UMAP1")
    axes[1].set_ylabel("UMAP2")
    cbar2 = plt.colorbar(scatter2, ax=axes[1], ticks=background_labels)
    cbar2.set_label(f"{target2} Cluster")

    # 輸出
    plt.tight_layout()
    if not os.path.exists(f'umap_output/{version}'):
        os.mkdir(f'umap_output/{version}')
    plt.savefig(f'umap_output/{version}/umap_dual_view_{target1}_{target2}.png', dpi=300)
    plt.close()


if __name__ == "__main__":
    version = short_id()
    create_umap("score", "mental", version)
    create_umap("score", "background", version)
    create_umap("mental", "background", version)
    create_umap("mental", "score", version)
    create_umap("background", "score", version)
    create_umap("background", "mental", version)
    print("UMAP visualization completed and saved.")
