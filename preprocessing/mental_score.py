from pyspark.sql.functions import col
from pyspark.sql.functions import abs

def compute_mental_score(df):
    return df.withColumn(
        "mental_score",
        _get_sleep_score(col("Sleep_Hours_per_Night")) +
        _get_attendance_score(col("Attendance (%)")) +
        _get_participation_score(col("Participation_Score")) +
        _get_extracurricular_activities_score(col("Extracurricular_Activities")) +
        _get_internet_access_score(col("Internet_Access"))
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
