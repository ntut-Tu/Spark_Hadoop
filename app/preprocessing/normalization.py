from pyspark.ml.feature import VectorAssembler, MinMaxScaler, StandardScaler
from pyspark.ml.functions import vector_to_array
from configs.enum_headers import CandidateColumns
from configs.feature_select import get_need_normalize_features

def _apply_custom_scaling(df, col_name):
    vec_assembler = VectorAssembler(inputCols=[col_name], outputCol=f"{col_name}_vec_std")
    scaler = StandardScaler(inputCol=f"{col_name}_vec_std", outputCol=f"{col_name}_std_scaled", withMean=True, withStd=True)

    df = vec_assembler.transform(df)
    model = scaler.fit(df)
    df = model.transform(df)
    df = df.withColumn(col_name, vector_to_array(f"{col_name}_std_scaled")[0])
    df = df.drop(f"{col_name}_vec_std", f"{col_name}_std_scaled")

    return df

def _apply_scaling_per_column(df, columns):
    for col_name in columns:
        vec_assembler = VectorAssembler(inputCols=[col_name], outputCol=f"{col_name}_vec")
        scaler = MinMaxScaler(inputCol=f"{col_name}_vec", outputCol=f"{col_name}_scaled")

        df = vec_assembler.transform(df)
        model = scaler.fit(df)
        df = model.transform(df)
        df = df.withColumn(col_name, vector_to_array(f"{col_name}_scaled")[0])
        df = df.drop(f"{col_name}_vec", f"{col_name}_scaled")
    return df

def apply_scaling(df):
    df = _apply_scaling_per_column(df, get_need_normalize_features())
    df = _apply_custom_scaling(df, CandidateColumns.study_hours_per_week)
    return df
