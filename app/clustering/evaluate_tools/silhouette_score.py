from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler

from configs.feature_select import get_mental_features_for_scoring, get_default_score_features, \
    get_background_features_for_scoring


def _features_assembler(df, feature_type):
    if feature_type == "background":
        print("background clustering:")
        assembler = VectorAssembler(inputCols=get_background_features_for_scoring(), outputCol="sil_score_features")
        return assembler.transform(df)
    elif feature_type == "score":
        print("score clustering:")
        assembler = VectorAssembler(inputCols=get_default_score_features(), outputCol="sil_score_features")
        return assembler.transform(df)
    elif feature_type == "mental":
        print("mental clustering:")
        assembler = VectorAssembler(inputCols=get_mental_features_for_scoring(), outputCol="sil_score_features")
        return assembler.transform(df)
    else:
        raise ValueError(f"Unknown feature type: {feature_type}")

def show_silhouette_score(df, predict_col, feature_type):
    df1 = _features_assembler(df, feature_type)
    evaluator = ClusteringEvaluator(
        predictionCol=predict_col,
        featuresCol="sil_score_features",
        metricName="silhouette",
        distanceMeasure="squaredEuclidean"
    )
    silhouette = evaluator.evaluate(df1)

    print(f"Silhouette Score: {silhouette}")

    return silhouette

