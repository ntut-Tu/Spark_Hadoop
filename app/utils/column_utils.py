from pyspark.sql.functions import when, col
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType, DoubleType, IntegerType

from configs.enum_headers import RawColumns


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

def get_raw_column_defaults():
    return {
        RawColumns.Student_ID.value: lit("None").cast(StringType()),
        RawColumns.First_Name.value: lit("None").cast(StringType()),
        RawColumns.Last_Name.value: lit("None").cast(StringType()),
        RawColumns.Email.value: lit("None").cast(StringType()),
        RawColumns.Gender.value: lit("Male").cast(StringType()),
        RawColumns.Age.value: lit(20).cast(IntegerType()),
        RawColumns.Department.value: lit("CS").cast(StringType()),
        RawColumns.Attendance_Percent.value: lit(80.0).cast(DoubleType()),
        RawColumns.Midterm_Score.value: lit(80).cast(DoubleType()),
        RawColumns.Final_Score.value: lit(80).cast(DoubleType()),
        RawColumns.Assignments_Avg.value: lit(80).cast(DoubleType()),
        RawColumns.Quizzes_Avg.value: lit(80).cast(DoubleType()),
        RawColumns.Participation_Score.value: lit(80).cast(DoubleType()),
        RawColumns.Projects_Score.value: lit(80).cast(DoubleType()),
        RawColumns.Total_Score.value: lit(80).cast(DoubleType()),
        RawColumns.Grade.value: lit(80).cast(StringType()),
        RawColumns.Study_Hours_per_Week.value: lit(3).cast(DoubleType()),
        RawColumns.Extracurricular_Activities.value: lit("Yes").cast(StringType()),
        RawColumns.Internet_Access_at_Home.value: lit("Yes").cast(StringType()),
        RawColumns.Parent_Education_Level.value: lit("None").cast(StringType()),
        RawColumns.Family_Income_Level.value: lit("Low").cast(StringType()),
        RawColumns.Stress_Level.value: lit(3).cast(DoubleType()),
        RawColumns.Sleep_Hours_per_Night.value: lit(8).cast(DoubleType()),
    }

def ensure_all_raw_columns(df):
    defaults = get_raw_column_defaults()
    for col_name, default_col in defaults.items():
        if col_name not in df.columns:
            df = df.withColumn(col_name, default_col)
        else:
            df = df.withColumn(col_name, when(col(col_name).isNull(), default_col).otherwise(col(col_name)))
    return df


