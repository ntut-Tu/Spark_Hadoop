from pyspark.ml.clustering import KMeans

def run(df, config):
    kmeans = KMeans(k=3, featuresCol="features", predictionCol="background_cluster")
    model = kmeans.fit(df)
    model.write().overwrite().save(config['model']['background_cluster'])
    return model.transform(df)
