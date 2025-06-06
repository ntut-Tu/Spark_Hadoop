from preprocessing.scoring.background_score import compute_background_score
from preprocessing.scoring.mental_score import new_compute_mental_score, compute_mental_score
from preprocessing.scoring.score_score import compute_score_score


def apply_scoring(df):
    # Apply mental score computation
    df = compute_mental_score(df)

    # Apply score score computation
    df = compute_score_score(df)

    # Apply background score computation
    df = compute_background_score(df)
    return df