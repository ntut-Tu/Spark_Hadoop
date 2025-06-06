from pyspark.sql.functions import col
from pyspark.sql.functions import abs

from clustering import mental_cluster
from clustering.evaluate_tools.weight_evaluate_tools import optimize_mental_score_weights
from configs.enum_headers import CandidateColumns
from preprocessing.normalization import _apply_custom_scaling


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


def new_compute_mental_score_v1(df):
    df1 = df.select("*")
    df1 = _apply_custom_scaling(df1, CandidateColumns.sleep_hours_per_night) #z-score
    df1 = _apply_custom_scaling(df1, CandidateColumns.attendance_percent)
    df1 = _apply_custom_scaling(df1, CandidateColumns.b_extracurricular_activities)
    df1 = _apply_custom_scaling(df1, CandidateColumns.b_internet_access_at_home)
    df1 = _apply_custom_scaling(df1, CandidateColumns.stress_level)
    return df1.withColumn(
        CandidateColumns.score_mental,
        col(CandidateColumns.sleep_hours_per_night)*0.3 +
        col(CandidateColumns.attendance_percent)*0.3 +
        col(CandidateColumns.b_extracurricular_activities)*0.05 +
        col(CandidateColumns.b_internet_access_at_home)*0.05 +
        col(CandidateColumns.stress_level)*0.35
    )

def new_compute_mental_score(df,config):

    features = [
        CandidateColumns.sleep_hours_per_night,
        CandidateColumns.attendance_percent,
        CandidateColumns.b_extracurricular_activities,
        CandidateColumns.b_internet_access_at_home,
        CandidateColumns.stress_level
    ]

    for feature in features:
        df = _apply_custom_scaling(df, feature)

    best_weights = optimize_mental_score_weights(
        df=df,
        features=features,
        cluster_func=mental_cluster.run,
        steps=6,
        config=config
    )

    weighted_expr = sum([
        w * col(f) for f, w in zip(features, best_weights)
    ])
    df = df.withColumn(CandidateColumns.score_mental, weighted_expr)
    df = _apply_custom_scaling(df, CandidateColumns.score_mental)
    print(f"new compute_mental_score: {CandidateColumns.score_mental}, best_weights: {best_weights}")
    return df



