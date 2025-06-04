from pyspark.sql.functions import col
from pyspark.sql.functions import abs

from configs.enum_headers import CandidateColumns


def compute_mental_score(df):
    return df.withColumn(
        CandidateColumns.score_mental,
        _get_sleep_score(col(CandidateColumns.sleep_hours_per_night)) +
        _get_attendance_score(col(CandidateColumns.attendance_percent)) +
        _get_extracurricular_activities_score(col(CandidateColumns.b_extracurricular_activities)) +
        _get_internet_access_score(col(CandidateColumns.b_internet_access_at_home)) +
        _get_stress_level_score(col(CandidateColumns.stress_level))
    )


def _get_sleep_score(hr_col):
    return 0.3 * (8 - abs(hr_col - 8)) / 8

def _get_attendance_score(p):
    return 0.2 * (p / 100)

def _get_extracurricular_activities_score(b):
    return 0.05 * b

def _get_internet_access_score(b):
    return 0.05 * b

def _get_stress_level_score(b):
    return 0.4 * b
