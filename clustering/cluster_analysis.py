def cross_tab(df, config):
    cross = df.crosstab("score_cluster", "background_cluster")
    cross.show()
    cross.write.mode("overwrite").csv("hdfs://hadoop-master:9000/data/processed/cluster_crosstab.csv")
