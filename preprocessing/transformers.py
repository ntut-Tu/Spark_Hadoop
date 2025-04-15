from pyspark.sql.functions import when, col
from pyspark.ml.feature import StringIndexer

def apply_transformations(df):
    df = df.withColumn("Extracurricular_Activities", when(col("Extracurricular_Activities") == "Yes", 1).otherwise(0))
    df = df.withColumn("Internet_Access", when(col("Internet_Access_at_Home") == "Yes", 1).otherwise(0))

    for colname in ["Parent_Education_Level", "Family_Income_Level"]:
        indexer = StringIndexer(inputCol=colname, outputCol=colname + "_Index")
        df = indexer.fit(df).transform(df)

    return df