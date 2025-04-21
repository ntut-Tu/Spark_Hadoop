def cross_tab(df1, df2, config):
    merged = df1.join(df2, on="student_id", how="inner")
    merged.printSchema()
    cross = merged.crosstab("score_cluster", "background_cluster")
    cross.show()
    cross.write.mode("overwrite").csv("hdfs://hadoop-master:9000/data/processed/cluster_crosstab")
