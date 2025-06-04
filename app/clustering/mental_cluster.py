from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.feature import VectorAssembler

from configs.feature_select import get_columns_for_mental_cluster


def run(df, config):
    input_cols = ["mental_score"] + get_columns_for_mental_cluster()
    assembler = VectorAssembler(inputCols=input_cols, outputCol="mental_vector")
    df = assembler.transform(df)

    kmeans = KMeans(k=3, featuresCol="mental_vector", predictionCol="mental_cluster")
    model = kmeans.fit(df)
    model.write().overwrite().save(config['model']['mental_cluster'])
    return model.transform(df)

def predict_with_mental_model(df, config):
    model_path = config['model']['mental_cluster']
    model = KMeansModel.load(model_path)

    input_cols = ["mental_score"] + get_columns_for_mental_cluster()
    assembler = VectorAssembler(inputCols=input_cols, outputCol="mental_vector")
    df = assembler.transform(df)

    df = model.transform(df)
    return df