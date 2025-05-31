import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql import Row

from preprocessing.pipeline import run_prediction_pipeline
from configs.enum_headers import RawColumns


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("TestKafkaPredictStream") \
        .master("local[*]") \
        .getOrCreate()


def get_test_schema():
    return StructType() \
        .add(RawColumns.Student_ID.value, StringType()) \
        .add(RawColumns.Gender.value, StringType()) \
        .add(RawColumns.Extracurricular_Activities.value, StringType()) \
        .add(RawColumns.Internet_Access_at_Home.value, StringType()) \
        .add(RawColumns.Family_Income_Level.value, StringType()) \
        .add(RawColumns.Parent_Education_Level.value, StringType()) \
        .add(RawColumns.Department.value, StringType()) \
        .add(RawColumns.Grade.value, StringType()) \
        .add(RawColumns.Study_Hours_per_Week.value, DoubleType()) \
        .add(RawColumns.Final_Score.value, DoubleType())


def test_run_prediction_pipeline_should_not_raise(spark):
    schema = get_test_schema()

    data = [Row(**{
        RawColumns.Student_ID.value: "S001",
        RawColumns.Gender.value: "Male",
        RawColumns.Extracurricular_Activities.value: "Yes",
        RawColumns.Internet_Access_at_Home.value: "Yes",
        RawColumns.Family_Income_Level.value: "Medium",
        RawColumns.Parent_Education_Level.value: "Bachelor's",
        RawColumns.Department.value: "CS",
        RawColumns.Grade.value: "B",
        RawColumns.Study_Hours_per_Week.value: 10.0,
        RawColumns.Final_Score.value: 85.0
    })]

    df = spark.createDataFrame(data, schema)

    try:
        run_prediction_pipeline(df, batch_id=123)
    except Exception as e:
        pytest.fail(f"run_prediction_pipeline raised an exception: {e}")
