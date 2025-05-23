from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType

def clean_column_names(df):
    cleaned_columns = [col.replace(" ", "_")
                          .replace("(", "")
                          .replace(")", "")
                          .replace("%", "percent")
                       for col in df.columns]
    return df.toDF(*cleaned_columns)

def convert_boolean_to_int(df):
    for field in df.schema.fields:
        if isinstance(field.dataType, BooleanType):
            df = df.withColumn(field.name, col(field.name).cast("int"))
    return df