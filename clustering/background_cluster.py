from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

from preprocessing.feature_select import get_normalized_columns


def run(df, config):
    input_cols = ["background_score"] + get_normalized_columns()
    assembler = VectorAssembler(inputCols=input_cols, outputCol="background_vector")
    df = assembler.transform(df)

    kmeans = KMeans(k=3, featuresCol="background_vector", predictionCol="background_cluster")
    model = kmeans.fit(df)
    model.write().overwrite().save(config['model']['background_cluster'])
    return model.transform(df)
