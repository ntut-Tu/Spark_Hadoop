from configs.enum_headers import CandidateColumns
from preprocessing.scoring.background_score import compute_background_score_ml_v1, compute_background_score_ml_v2
from preprocessing.scoring.mental_score import compute_mental_score_ml_v1, compute_mental_score_ml_v2
from preprocessing.scoring.score_score import compute_score_score_ml, compute_score_score, compute_score_score_v1, \
    compute_score_default


def apply_scoring(df):
    # Apply score score computation
    df = compute_score_score_ml(df)

    # df.select(CandidateColumns.final_performance_score).describe().show()
    # Apply mental score computation
    df = compute_mental_score_ml_v2(df)

    # Apply background score computation
    df = compute_background_score_ml_v1(df)
    return df