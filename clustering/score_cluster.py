from pyspark.ml.clustering import KMeans

def run(df, config):
    from pyspark.ml.feature import VectorAssembler

    assembler = VectorAssembler(inputCols=["Total_Score"], outputCol="score_vector")
    df = assembler.transform(df)

    kmeans = KMeans(k=3, featuresCol="score_vector", predictionCol="score_cluster")
    model = kmeans.fit(df)
    model.write().overwrite().save(config['model']['score_cluster'])
    return model.transform(df)
