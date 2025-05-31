from pydantic import BaseModel, Field
from typing import Literal


class PredictRequest(BaseModel):
    Student_ID: str = Field(...)
    Gender: Literal["Male", "Female"]
    Extracurricular_Activities: Literal["Yes", "No"]
    Internet_Access_at_Home: Literal["Yes", "No"]
    Family_Income_Level: Literal["High", "Medium", "Low"]
    Parent_Education_Level: Literal["None", "High School", "Bachelor's", "Master's", "PhD"]
    Department: Literal["Mathematics", "Business", "Engineering", "CS"]
    Grade: Literal["A", "B", "C", "D", "F"]
    Study_Hours_per_Week: float
    Final_Score: float

