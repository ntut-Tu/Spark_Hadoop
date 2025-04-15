def cross_tab(df, config):
    cross = df.crosstab("score_cluster", "background_cluster")
    cross.show()
    cross.write.mode("overwrite").csv("data/processed/cluster_crosstab.csv")
