from pyspark.sql.functions import col
from pyspark.sql.functions import abs

from clustering import mental_cluster
from clustering.evaluate_tools.weight_evaluate_tools import optimize_mental_score_weights
from configs.enum_headers import CandidateColumns
from preprocessing.normalization import _apply_custom_scaling


def compute_score_score(df):
    df1 = df.withColumn(
        CandidateColumns.final_performance_score,
        (col(CandidateColumns.midterm_score))*0.3 +
        (col(CandidateColumns.final_score))*0.4 +
        (col(CandidateColumns.assignments_avg))*0.1 +
        (col(CandidateColumns.quizzes_avg))*0.1 +
        col(CandidateColumns.projects_score)*0.1
    )
    df1 = _apply_custom_scaling(df1, CandidateColumns.final_performance_score)  # z-score
    return df1