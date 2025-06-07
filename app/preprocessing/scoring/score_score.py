from pyspark.sql.functions import col

from configs.enum_headers import CandidateColumns
from preprocessing.normalization import _apply_custom_scaling
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression


def compute_score_score(df):
    df1 = df.withColumn(
        CandidateColumns.final_performance_score,
        (col(CandidateColumns.midterm_score)) * 0.3 +
        (col(CandidateColumns.final_score)) * 0.4 +
        (col(CandidateColumns.assignments_avg)) * 0.1 +
        (col(CandidateColumns.quizzes_avg)) * 0.1 +
        col(CandidateColumns.projects_score) * 0.1
    )
    df1 = _apply_custom_scaling(df1, CandidateColumns.final_performance_score)  # z-score
    return df1

def compute_score_default(df):
    df1 = df.withColumn(
        CandidateColumns.final_performance_score,
        col(CandidateColumns.total_score)
    )
    df1 = _apply_custom_scaling(df1, CandidateColumns.final_performance_score)  # z-score
    return df1

def compute_score_score_v1(df):
    score_expr = (
            col(CandidateColumns.midterm_score) * -0.002234440752138049 +
            col(CandidateColumns.final_score) * 0.017000616092716497 +
            col(CandidateColumns.assignments_avg) * 0.019689447614962528 +
            col(CandidateColumns.quizzes_avg) * -0.005575354921697527 +
            col(CandidateColumns.projects_score) * -0.027351895636555393
    )

    df1 = df.withColumn(CandidateColumns.final_performance_score, score_expr)
    df1 = _apply_custom_scaling(df1, CandidateColumns.final_performance_score)
    return df1


def _learn_score_weight(df, features):
    feature_output_col = "score_ml_features"
    assembler = VectorAssembler(inputCols=features, outputCol=feature_output_col)
    df_vector = assembler.transform(df)

    lr = LinearRegression(
        featuresCol=feature_output_col,
        labelCol=CandidateColumns.total_score,
        # elasticNetParam = 0.6,
        # regParam = 0.01
    )
    model = lr.fit(df_vector)
    df.drop(feature_output_col)

    weights = model.coefficients.toArray()
    print("📊 學出的分數加權:")
    print(dict(zip(features, weights)))
    return weights


def compute_score_score_ml(df):
    features = [
        CandidateColumns.midterm_score,
        CandidateColumns.final_score,
        CandidateColumns.assignments_avg,
        CandidateColumns.quizzes_avg,
        CandidateColumns.projects_score
    ]
    weights = _learn_score_weight(df, features)

    score_expr = sum([
        weight * col(col_name)
        for col_name, weight in zip(features, weights)
    ])
    return df.withColumn(CandidateColumns.final_performance_score, score_expr)
