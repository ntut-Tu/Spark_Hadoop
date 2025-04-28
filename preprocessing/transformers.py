from pyspark.sql.functions import when, col
from pyspark.ml.feature import StringIndexer

def apply_transformations(df):
    df = df.withColumn("Extracurricular_Activities", when(col("Extracurricular_Activities") == "Yes", 1).otherwise(0))
    df = df.withColumn("Internet_Access", when(col("Internet_Access_at_Home") == "Yes", 1).otherwise(0))

    df = df.withColumn(
        "Family_Income_Level_Index",
        when(col("Family_Income_Level") == "High", 3)
        .when(col("Family_Income_Level") == "Medium", 2)
        .when(col("Family_Income_Level") == "Low", 1)
        .otherwise(0)
    )
    df = df.withColumn(
        "Parent_Education_Level_Index",
        when(col("Parent_Education_Level") == "None", 0)
        .when(col("Parent_Education_Level") == "High School", 1)
        .when(col("Parent_Education_Level") == "Bachelor's", 2)
        .when(col("Parent_Education_Level") == "Master's", 3)
        .when(col("Parent_Education_Level") == "PhD", 4)
        .otherwise(0)
    )

    return df