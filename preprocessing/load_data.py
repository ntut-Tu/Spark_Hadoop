def read_raw_data(spark, path):
    return spark.read.option("header", True).option("inferSchema", True).csv(path)
