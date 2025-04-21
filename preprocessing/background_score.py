from pyspark.sql.functions import col, lit, min as spark_min, max as spark_max

def compute_background_score(df):
    columns_with_weights = {
        "Study_Hours_per_Week": 0.25,
        "Parent_Education_Level_Index": 0.25,
        "Family_Income_Level_Index": 0.25,
    }

    agg_exprs = []
    for col_name in columns_with_weights:
        agg_exprs.append(spark_min(col_name).alias(f"{col_name}_min"))
        agg_exprs.append(spark_max(col_name).alias(f"{col_name}_max"))

    min_max_row = df.agg(*agg_exprs).collect()[0]

    min_max_map = {
        col_name: (
            float(min_max_row[f"{col_name}_min"]),
            float(min_max_row[f"{col_name}_max"])
        )
        for col_name in columns_with_weights
    }

    score_expr = sum([
        weight * ((col(col_name) - lit(min_val)) / lit(max_val - min_val))
        for col_name, weight in columns_with_weights.items()
        for (min_val, max_val) in [min_max_map[col_name]]
    ])

    score_expr += 0.25 * col("Extracurricular_Activities")

    return df.withColumn("background_score", score_expr)
