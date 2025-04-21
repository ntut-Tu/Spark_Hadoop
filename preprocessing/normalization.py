from pyspark.ml.feature import VectorAssembler, StandardScaler

from config.feature_select import get_background_features


def apply_scaling(df):
    input_cols = get_background_features()

    assembler = VectorAssembler(inputCols=input_cols, outputCol="features_vector")
    df = assembler.transform(df)

    scaler = StandardScaler(inputCol="features_vector", outputCol="features", withStd=True, withMean=True)
    model = scaler.fit(df)
    return model.transform(df)
