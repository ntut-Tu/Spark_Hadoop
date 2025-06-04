from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import ClusteringEvaluator

from configs.feature_select import get_columns_for_background_cluster


def run(df, config):
    input_cols = ["background_score"] + get_columns_for_background_cluster()
    assembler = VectorAssembler(inputCols=input_cols, outputCol="background_vector")
    df = assembler.transform(df)
    kmeans = KMeans(k=3, featuresCol="background_vector", predictionCol="background_cluster", seed=123, initMode="k-means||")
    model = kmeans.fit(df)
    model.write().overwrite().save(config['model']['background_cluster'])
    return model.transform(df)

def predict_with_background_model(df, config):
    model_path = config['model']['background_cluster']
    model = KMeansModel.load(model_path)

    input_cols = ["background_score"] + get_columns_for_background_cluster()
    assembler = VectorAssembler(inputCols=input_cols, outputCol="background_vector")
    df = assembler.transform(df)

    df = model.transform(df)
    return df