from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler

from configs.feature_select import get_score_features, get_testing_score_features


def _mode_helper(mode):
    if mode == "default":
        return get_score_features()
    elif mode == "test":
        return get_testing_score_features()

def run(df, config):
    assembler = VectorAssembler(inputCols=_mode_helper("default"), outputCol="score_vector")
    df = assembler.transform(df)

    kmeans = KMeans(k=3, featuresCol="score_vector", predictionCol="score_cluster")
    model = kmeans.fit(df)
    model.write().overwrite().save(config['model']['score_cluster'])
    return model.transform(df)


def predict_with_score_model(df, config):
    model_path = config['model']['score_cluster']
    model = KMeansModel.load(model_path)

    assembler = VectorAssembler(inputCols=get_score_features(), outputCol="score_vector")
    df = assembler.transform(df)

    df = model.transform(df)
    return df
