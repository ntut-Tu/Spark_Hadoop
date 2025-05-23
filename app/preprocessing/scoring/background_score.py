from pyspark.sql.functions import col

from configs.enum_headers import CandidateColumns


def compute_background_score(df):
    weights = {
        CandidateColumns.study_hours_per_week: 1,
        CandidateColumns.b_parent_edu_none: 0.4,
        CandidateColumns.b_parent_edu_bachelor: 0.6,
        CandidateColumns.b_parent_edu_master: 0.8,
        CandidateColumns.b_parent_edu_phd: 1,
        CandidateColumns.b_family_inc_medium: 0.5,
        CandidateColumns.b_family_inc_high: 0.8,
        CandidateColumns.b_family_inc_low: 0.3,
        CandidateColumns.b_extracurricular_activities: 0.5
    }

    score_expr = sum([
        weight * col(col_name)
        for col_name, weight in weights.items()
    ])

    return df.withColumn(CandidateColumns.score_background, score_expr)
