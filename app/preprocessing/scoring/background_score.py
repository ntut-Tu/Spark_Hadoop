from pyspark.sql.functions import col

from configs.enum_headers import CandidateColumns
from preprocessing.normalization import _apply_custom_scaling


def _compute_personal_background_score(df):
    weights = {
        CandidateColumns.study_hours_per_week: 3,
        CandidateColumns.b_extracurricular_activities: 1
    }

    score_expr = sum([
        weight * col(col_name)
        for col_name, weight in weights.items()
    ])
    return df.withColumn(CandidateColumns.score_personal_background, score_expr*0.33)

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
    return df.withColumn(CandidateColumns.score_edu_background, score_expr*0.3)

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
        score_expr*0.3,
        col(CandidateColumns.score_personal_background),
        col(CandidateColumns.score_edu_background)
    ])
    return df.withColumn(CandidateColumns.score_background, score_expr)


def new_compute_background_score(df):
    df1 = df.select("*")

    personal_weights = {
        CandidateColumns.study_hours_per_week: 1.0,
        CandidateColumns.b_extracurricular_activities: 0.5
    }
    personal_expr = sum([
        weight * col(col_name)
        for col_name, weight in personal_weights.items()
    ])
    df1 = df1.withColumn(CandidateColumns.score_personal_background, personal_expr)

    edu_weights = {
        CandidateColumns.b_parent_edu_none: 0,
        CandidateColumns.b_parent_edu_high_school: 1,
        CandidateColumns.b_parent_edu_bachelor: 2,
        CandidateColumns.b_parent_edu_master: 3,
        CandidateColumns.b_parent_edu_phd: 4
    }
    edu_expr = sum([
        weight * col(col_name)
        for col_name, weight in edu_weights.items()
    ])
    df1 = df1.withColumn(CandidateColumns.score_edu_background, edu_expr)

    income_weights = {
        CandidateColumns.b_family_inc_low: 0,
        CandidateColumns.b_family_inc_medium: 1,
        CandidateColumns.b_family_inc_high: 2
    }
    income_expr = sum([
        weight * col(col_name)
        for col_name, weight in income_weights.items()
    ])

    total_background_expr = (
        0.3 * col(CandidateColumns.score_personal_background) +
        0.3 * col(CandidateColumns.score_edu_background) +
        0.4 * income_expr
    )

    df1 = df1.withColumn(CandidateColumns.score_background, total_background_expr)

    df1 = _apply_custom_scaling(df1, CandidateColumns.score_background) #z-score scaling

    return df1
