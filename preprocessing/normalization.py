from pyspark.ml.feature import VectorAssembler, StandardScaler

from preprocessing.feature_select import get_background_features


def apply_scaling(df):
    assembler = VectorAssembler(
        inputCols=["Study_Hours_per_Week", "Parent_Education_Level_Index", "Family_Income_Level_Index", "mental_score"],
        outputCol="features_raw"
    )
    df = assembler.transform(df)

    scaler = StandardScaler(inputCol=get_background_features(), outputCol="features", withStd=True, withMean=True)
    model = scaler.fit(df)
    return model.transform(df)
