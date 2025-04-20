def cross_tab(df1, df2, config):
    dfn = df2.drop("score_cluster").cache()
    merged = df1.join(dfn, on="student_id", how="inner")
    merged.select("student_id","background_cluster","score_cluster").show(5)
    merged.printSchema()
    merged.write.mode("overwrite").parquet("hdfs://hadoop-master:9000/data/processed/merged")
    cross = merged.crosstab("score_cluster", "background_cluster")
    cross.show()
    cross.write.mode("overwrite").csv("hdfs://hadoop-master:9000/data/processed/cluster_crosstab")
