from typing import Literal, Optional
from pydantic import BaseModel, Field

class PredictRequest(BaseModel):
    # modeB fields
    Student_ID: Optional[str] = Field(default=None, alias="Student_ID")
    Gender: Optional[Literal["Male", "Female"]] = Field(default=None, alias="Gender")
    Extracurricular_Activities: Optional[Literal["Yes", "No"]] = Field(default=None, alias="Extracurricular_Activities")
    Internet_Access_at_Home: Optional[Literal["Yes", "No"]] = Field(default=None, alias="Internet_Access_at_Home")
    Family_Income_Level: Optional[Literal["High", "Medium", "Low"]] = Field(default=None, alias="Family_Income_Level")
    Parent_Education_Level: Optional[Literal["None", "High School", "Bachelor's", "Master's", "PhD"]] = Field(default=None, alias="Parent_Education_Level")
    Department: Optional[Literal["Mathematics", "Business", "Engineering", "CS"]] = Field(default=None, alias="Department")
    Grade: Optional[Literal["A", "B", "C", "D", "F"]] = Field(default=None, alias="Grade")
    Study_Hours_per_Week: Optional[float] = Field(default=None, alias="Study_Hours_per_Week")
    Total_Score: Optional[float] = Field(default=None, alias="Total_Score")
    # modeA fields
    Sleep_Hours_per_Night: Optional[float] = Field(default=None, alias="Sleep_Hours_per_Night")
    Attendance_Percent: Optional[float] = Field(default=None, alias="Attendance (%)")
    Stress_Level: Optional[float] = Field(default=None, alias="Stress_Level (1-10)")

    Projects_Score: Optional[float] = Field(default=None, alias="Projects_Score")
    Quizzes_Avg: Optional[float] = Field(default=None, alias="Quizzes_Avg")
    class Config:
        allow_population_by_field_name = True
