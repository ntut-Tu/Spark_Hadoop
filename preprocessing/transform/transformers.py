from pyspark.sql.functions import when, col
from pyspark.ml.feature import StringIndexer

from config.enum_headers import RawColumns, RenamedColumns, CandidateColumns
from preprocessing.transform.mappings import RAW_TO_INTERM_MAP, RENAMED_TO_CANDIDATE_MAP


def _raw_to_renamed(raw_col: RawColumns) -> RenamedColumns:
    return RAW_TO_INTERM_MAP.get(raw_col)

def _renamed_to_candidate(interm_col: RenamedColumns) -> CandidateColumns:
    return RENAMED_TO_CANDIDATE_MAP.get(interm_col)

def apply_raw_column_renaming(df):
    rename_map = {raw.value: interm.value for raw, interm in RAW_TO_INTERM_MAP.items()}
    return df.select([col(c).alias(rename_map.get(c, c)) for c in df.columns])

def apply_to_candidate_transformations(df):
    # Gender → one-hot
    df = df.withColumn(CandidateColumns.b_gender_male.value, col(RawColumns.Gender.value) == "Male")
    df = df.withColumn(CandidateColumns.b_gender_female.value, col(RawColumns.Gender.value) == "Female")

    # Extracurricular Activities → bool
    df = df.withColumn(CandidateColumns.b_extracurricular_activities.value,
                       col(RawColumns.Extracurricular_Activities.value) == "Yes")

    # Internet Access → bool
    df = df.withColumn(CandidateColumns.b_internet_access_at_home.value,
                       col(RawColumns.Internet_Access_at_Home.value) == "Yes")

    # Family Income Level → one-hot
    df = df.withColumn(CandidateColumns.b_family_inc_high.value, col(RawColumns.Family_Income_Level.value) == "High")
    df = df.withColumn(CandidateColumns.b_family_inc_medium.value,
                       col(RawColumns.Family_Income_Level.value) == "Medium")
    df = df.withColumn(CandidateColumns.b_family_inc_low.value, col(RawColumns.Family_Income_Level.value) == "Low")

    # Parent Education Level → one-hot
    df = df.withColumn(CandidateColumns.b_parent_edu_none.value, col(RawColumns.Parent_Education_Level.value) == "None")
    df = df.withColumn(CandidateColumns.b_parent_edu_high_school.value,
                       col(RawColumns.Parent_Education_Level.value) == "High School")
    df = df.withColumn(CandidateColumns.b_parent_edu_bachelor.value,
                       col(RawColumns.Parent_Education_Level.value) == "Bachelor's")
    df = df.withColumn(CandidateColumns.b_parent_edu_master.value,
                       col(RawColumns.Parent_Education_Level.value) == "Master's")
    df = df.withColumn(CandidateColumns.b_parent_edu_phd.value, col(RawColumns.Parent_Education_Level.value) == "PhD")

    # df = _renamed_to_candidate(df)
    return df


# def apply_transformations(df):
#     df = df.withColumn("Extracurricular_Activities", when(col("Extracurricular_Activities") == "Yes", 1).otherwise(0))
#     df = df.withColumn("Internet_Access", when(col("Internet_Access_at_Home") == "Yes", 1).otherwise(0))
#
#     df = df.withColumn(
#         "Family_Income_Level_Index",
#         when(col("Family_Income_Level") == "High", 3)
#         .when(col("Family_Income_Level") == "Medium", 2)
#         .when(col("Family_Income_Level") == "Low", 1)
#         .otherwise(0)
#     )
#     df = df.withColumn(
#         "Parent_Education_Level_Index",
#         when(col("Parent_Education_Level") == "None", 0)
#         .when(col("Parent_Education_Level") == "High School", 1)
#         .when(col("Parent_Education_Level") == "Bachelor's", 2)
#         .when(col("Parent_Education_Level") == "Master's", 3)
#         .when(col("Parent_Education_Level") == "PhD", 4)
#         .otherwise(0)
#     )
#
#     return df