from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def get_background_cluster_mappings():
    return [
        "growing_opportunities",  #0
        "abundant_opportunities", #1
        "expanding_opportunities" #2
    ]

def get_score_cluster_mappings():
    return [
        "low_score",   #0
        "high_score",  #1
        "normal_score",#2
    ]

def get_mental_cluster_mappings():
    return [
        "high_stress",  #0
        "normal_stress",#1
        "low_stress"    #2
    ]


def label_mapping(df):
    background_labels = get_background_cluster_mappings()
    mental_labels = get_mental_cluster_mappings()
    score_labels = get_score_cluster_mappings()

    background_udf = udf(lambda idx: background_labels[idx], StringType())
    mental_udf = udf(lambda idx: mental_labels[idx], StringType())
    score_udf = udf(lambda idx: score_labels[idx], StringType())

    df = df.withColumn("background_cluster_label", background_udf(df["background_cluster"]))
    df = df.withColumn("mental_cluster_label", mental_udf(df["mental_cluster"]))
    df = df.withColumn("score_cluster_label", score_udf(df["score_cluster"]))

    return df
