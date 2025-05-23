from pyspark.ml.feature import VectorAssembler, MinMaxScaler
from pyspark.ml.functions import vector_to_array
from pyspark.sql.functions import col
from config.feature_select import get_need_normalize_features


def apply_scaling(df):
    input_cols = get_need_normalize_features()

    print(f"Scaling columns: {input_cols}")
    df.show(5, truncate=False)
    # 組合成 features_vector
    assembler = VectorAssembler(inputCols=input_cols, outputCol="features_vector")
    df = assembler.transform(df)

    # MinMax Scaling
    scaler = MinMaxScaler(inputCol="features_vector", outputCol="features_scaled")
    model = scaler.fit(df)
    df = model.transform(df)

    # vector 拆成 array，再還原成原本欄位
    df = df.withColumn("features_array", vector_to_array("features_scaled"))

    for i, col_name in enumerate(input_cols):
        df = df.withColumn(col_name, col("features_array")[i])

    # 移除中間欄位
    return df.drop("features_vector", "features_scaled", "features_array")
