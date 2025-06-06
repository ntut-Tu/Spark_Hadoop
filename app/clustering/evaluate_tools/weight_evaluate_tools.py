import itertools
import numpy as np
from pyspark.sql.functions import col
from clustering.evaluate_tools.silhouette_score import show_silhouette_score
from configs.enum_headers import CandidateColumns
from preprocessing.normalization import _apply_custom_scaling


def _generate_weight_combinations(n_features, steps=5):
    """
    產生所有 weights 組合，使得總和為1。
    每個 weight 在 [0,1] 間隔 steps 等級（含0與1）
    """
    grid = np.linspace(0, 1, steps)
    candidates = [
        comb for comb in itertools.product(grid, repeat=n_features)
        if abs(sum(comb) - 1.0) < 1e-6
    ]
    print(f"✅ Generated {len(candidates)} weight combinations for {n_features} features.")
    # print(f"📊 Combinations: {candidates[:5]}...")  # 只顯示前5個組合
    return candidates


def _evaluate_mental_score_with_weights(df, features, weights, cluster_func, config):
    # print(f"🔍 Evaluating weights: {dict(zip(features, weights))}")
    expr = sum([w * col(f) for f, w in zip(features, weights)])
    if CandidateColumns.score_mental in df.columns:
        df = df.drop(CandidateColumns.score_mental)
    df1 = df.withColumn(CandidateColumns.score_mental, expr)
    df1 = _apply_custom_scaling(df1, CandidateColumns.score_mental)

    df1 = cluster_func(df1, config)

    # ✅ 評估 score
    score = show_silhouette_score(df1, "mental_cluster", feature_type="mental")
    return score


def optimize_mental_score_weights(df, features, steps, cluster_func, config):
    combinations = _generate_weight_combinations(len(features), steps)
    best_score = 0.1
    best_weights = None

    for weights in combinations:
        score = _evaluate_mental_score_with_weights(df, features, weights, cluster_func, config)
        if score > best_score:
            best_score = score
            best_weights = weights
            print(f"🔝 New best score: {best_score:.4f} with weights: {dict(zip(features, weights))}")


    print(f"✅ Best silhouette score: {best_score:.4f}")
    print(f"📊 Best weights: {dict(zip(features, best_weights))}")
    return best_weights
