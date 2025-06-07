from pyspark.sql.functions import col

from configs.enum_headers import CandidateColumns
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from itertools import combinations


def _compute_personal_background_score(df):
    weights = {
        CandidateColumns.study_hours_per_week: 3,
        CandidateColumns.b_extracurricular_activities: 1
    }

    score_expr = sum([
        weight * col(col_name)
        for col_name, weight in weights.items()
    ])
    return df.withColumn(CandidateColumns.score_personal_background, score_expr * 0.33)


def _compute_edu_background_score(df):
    weights = {
        CandidateColumns.b_parent_edu_none: 0,
        CandidateColumns.b_parent_edu_high_school: 1,
        CandidateColumns.b_parent_edu_bachelor: 2,
        CandidateColumns.b_parent_edu_master: 3,
        CandidateColumns.b_parent_edu_phd: 5
    }

    score_expr = sum([
        weight * col(col_name)
        for col_name, weight in weights.items()
    ])
    return df.withColumn(CandidateColumns.score_edu_background, score_expr * 0.3)


def compute_background_score(df):
    df = _compute_personal_background_score(df)
    df = _compute_edu_background_score(df)
    weights = {
        CandidateColumns.b_family_inc_medium: 3,
        CandidateColumns.b_family_inc_high: 5,
        CandidateColumns.b_family_inc_low: 0,
    }

    score_expr = sum([
        weight * col(col_name)
        for col_name, weight in weights.items()
    ])
    score_expr = sum([
        score_expr * 0.3,
        col(CandidateColumns.score_personal_background),
        col(CandidateColumns.score_edu_background)
    ])
    return df.withColumn(CandidateColumns.score_background, score_expr)


def compute_background_score_v2(df):
    weights = {
        CandidateColumns.study_hours_per_week: -0.002529795554347102,
        CandidateColumns.b_extracurricular_activities: -0.004145213139607821,
        CandidateColumns.b_parent_edu_none: 0.004247490360813498,
        CandidateColumns.b_parent_edu_high_school: 0.0007891966766126794,
        CandidateColumns.b_parent_edu_bachelor: -0.013837450392555205,
        CandidateColumns.b_parent_edu_master: 0.0032952649996239145,
        CandidateColumns.b_parent_edu_phd: 0.005616730404021881,
        CandidateColumns.b_family_inc_low: -0.0006398221065081909,
        CandidateColumns.b_family_inc_medium: -0.002103960346655353,
        CandidateColumns.b_family_inc_high: 0.0027756665626751985
    }

    background_expr = sum([
        weight * col(col_name)
        for col_name, weight in weights.items()
    ])

    return df.withColumn(CandidateColumns.score_background, background_expr)


def _learn_background_weight_v1(df, features):
    feature_output_col = "background_ml_features"
    assembler = VectorAssembler(inputCols=features, outputCol=feature_output_col)
    df_vector = assembler.transform(df)

    lr = LinearRegression(
        featuresCol=feature_output_col,
        labelCol=CandidateColumns.final_performance_score,
        elasticNetParam=0.6,
        regParam=0.005
    )
    model = lr.fit(df_vector)
    df.drop(feature_output_col)

    weights = model.coefficients.toArray()
    print("background weights:")
    print(dict(zip(features, weights)))
    return weights


def compute_background_score_ml_v1(df):
    features = [
        CandidateColumns.study_hours_per_week,
        CandidateColumns.b_extracurricular_activities,
        CandidateColumns.int_parent_edu,
        CandidateColumns.int_family_inc,
    ]

    weights = _learn_background_weight_v1(df, features)
    score_expr = sum([
        weight * col(col_name)
        for col_name, weight in zip(features, weights)
    ])
    return df.withColumn(CandidateColumns.score_background, score_expr)


def _generate_interaction_features(df, features):
    interaction_features = []
    for f1, f2 in combinations(features, 2):
        name = f"{f1}_X_{f2}"
        df = df.withColumn(name, col(f1) * col(f2))
        interaction_features.append(name)
    return df, interaction_features


def _learn_background_weight_v2(df, base_features):
    df, interaction_features = _generate_interaction_features(df, base_features)
    all_features = base_features + interaction_features
    feature_output_col = "background_ml_features"

    assembler = VectorAssembler(inputCols=all_features, outputCol=feature_output_col)
    df_vector = assembler.transform(df)

    lr = LinearRegression(
        featuresCol=feature_output_col,
        labelCol=CandidateColumns.final_performance_score,
        elasticNetParam=0.6,  # 可以調 0~1
        regParam=0.005  # L1 會自動濾掉不重要特徵
    )
    model = lr.fit(df_vector)

    weights = model.coefficients.toArray()
    print("📊 學出的背景加權 (含交互特徵):")
    print(dict(zip(all_features, weights)))
    return df_vector, zip(all_features, weights)  # 回傳配對值


def compute_background_score_ml_v2(df):
    base_features = [
        CandidateColumns.study_hours_per_week,
        CandidateColumns.b_extracurricular_activities,
        CandidateColumns.int_parent_edu,
        CandidateColumns.int_family_inc,
    ]

    df, weighted_features = _learn_background_weight_v2(df, base_features)

    score_expr = sum([
        weight * col(col_name)
        for col_name, weight in weighted_features
    ])

    return df.withColumn(CandidateColumns.score_background, score_expr)
