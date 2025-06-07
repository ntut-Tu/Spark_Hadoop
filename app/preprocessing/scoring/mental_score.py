from pyexpat import features
from pyspark.sql.functions import col
from pyspark.sql.functions import abs

from configs.enum_headers import CandidateColumns
from preprocessing.normalization import _apply_custom_scaling
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression


def compute_mental_score(df):
    return df.withColumn(
        CandidateColumns.score_mental,
        _get_sleep_score(col(CandidateColumns.sleep_hours_per_night)) +
        _get_attendance_score(col(CandidateColumns.attendance_percent)) +
        _get_extracurricular_activities_score(col(CandidateColumns.b_extracurricular_activities)) +
        _get_internet_access_score(col(CandidateColumns.b_internet_access_at_home)) +
        col(CandidateColumns.stress_level) / 10 * 0.4
    )


def _get_sleep_score(hr_col):
    return 0.3 * (8 - abs(hr_col - 8)) / 8


def _get_attendance_score(p):
    return 0.3 * (p / 100)


def _get_participation_score(s):
    return 0.3 * ((10 - s) / 9)


def _get_extracurricular_activities_score(b):
    return 0.05 * b


def _get_internet_access_score(b):
    return 0.05 * b


def compute_mental_score_v1(df):
    df1 = df.select("*")
    df1 = _apply_custom_scaling(df1, CandidateColumns.sleep_hours_per_night)  # z-score
    df1 = _apply_custom_scaling(df1, CandidateColumns.attendance_percent)
    df1 = _apply_custom_scaling(df1, CandidateColumns.b_extracurricular_activities)
    df1 = _apply_custom_scaling(df1, CandidateColumns.b_internet_access_at_home)
    df1 = _apply_custom_scaling(df1, CandidateColumns.stress_level)
    return df1.withColumn(
        CandidateColumns.score_mental,
        col(CandidateColumns.sleep_hours_per_night) * 0.023 +
        col(CandidateColumns.attendance_percent) * 0 +
        col(CandidateColumns.b_extracurricular_activities) * 0 +
        col(CandidateColumns.b_internet_access_at_home) * 0.00582 +
        col(CandidateColumns.stress_level) * 0.0172
    )


def _learn_mental_weight_v1(df, features):
    feature_output_col = "mental_ml_features"
    assembler = VectorAssembler(inputCols=features, outputCol=feature_output_col)
    df_vector = assembler.transform(df)

    lr = LinearRegression(
        featuresCol=feature_output_col,
        labelCol=CandidateColumns.final_performance_score,
        # elasticNetParam=0.6,
        # regParam=0.005
    )
    model = lr.fit(df_vector)
    df.drop(feature_output_col)

    weights = model.coefficients.toArray()
    print("mental weights:")
    print(dict(zip(features, weights)))
    return weights


def compute_mental_score_ml_v1(df):
    features = [
        CandidateColumns.sleep_hours_per_night,
        CandidateColumns.attendance_percent,
        CandidateColumns.b_extracurricular_activities,
        CandidateColumns.b_internet_access_at_home,
        CandidateColumns.stress_level
    ]

    weights = _learn_mental_weight_v1(df, features)
    score_expr = sum([
        weight * col(col_name)
        for col_name, weight in zip(features, weights)
    ])
    return df.withColumn(CandidateColumns.score_mental, score_expr)

def _learn_mental_weight_v2(df, features):
    feature_output_col = "mental_ml_features"
    assembler = VectorAssembler(inputCols=features, outputCol=feature_output_col)
    df_vector = assembler.transform(df)

    lr = LinearRegression(
        featuresCol=feature_output_col,
        labelCol=CandidateColumns.final_performance_score,
        # elasticNetParam=0.6,
        # regParam=0.005
    )
    model = lr.fit(df_vector)
    df.drop(feature_output_col)

    weights = model.coefficients.toArray()
    print("mental weights:")
    print(dict(zip(features, weights)))
    return weights


def compute_mental_score_ml_v2(df):
    df = df.withColumn("mental_sleep", _get_sleep_score(col(CandidateColumns.sleep_hours_per_night)))
    df = df.withColumn("mental_attendance", _get_attendance_score(col(CandidateColumns.attendance_percent)))
    df = df.withColumn("mental_extracurricular",
                       _get_extracurricular_activities_score(col(CandidateColumns.b_extracurricular_activities)))
    df = df.withColumn("mental_internet", _get_internet_access_score(col(CandidateColumns.b_internet_access_at_home)))
    df = df.withColumn("mental_stress", col(CandidateColumns.stress_level) / 10 * 0.4)

    features = [
        "mental_sleep",
        "mental_attendance",
        "mental_extracurricular",
        "mental_internet",
        "mental_stress"
    ]

    weights = _learn_mental_weight_v2(df, features)
    score_expr = sum([
        weight * col(col_name)
        for col_name, weight in zip(features, weights)
    ])
    return df.withColumn(CandidateColumns.score_mental, score_expr)