from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import StringType

def cross_tab(df1, df2, config):
    merged = df1.join(df2, on="student_id", how="inner")

    cross = merged.crosstab("score_cluster", "background_cluster")

    def label_score(score):
        return {
            "0": "Low Score",
            "1": "Mid Score",
            "2": "High Score"
        }.get(score, f"Cluster {score}")

    label_udf = udf(label_score, StringType())
    cross = cross.withColumn("Score Cluster", label_udf(col("score_cluster_background_cluster")))

    cross = cross.withColumn("sort_key",
        when(col("Score Cluster") == "Low Score", 0)
        .when(col("Score Cluster") == "Mid Score", 1)
        .when(col("Score Cluster") == "High Score", 2)
    )

    ordered_cols = ["Score Cluster"] + [c for c in cross.columns if c not in ["Score Cluster", "score_cluster_background_cluster", "sort_key"]]
    cross = cross.orderBy("sort_key").select(*ordered_cols)

    cross.show()
    cross.write.mode("overwrite").csv("hdfs://hadoop-master:9000/data/processed/cluster_crosstab")
