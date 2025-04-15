from pyspark.sql.functions import col

def compute_mental_score(df):
    return df.withColumn(
        "mental_score",
        0.3 * (col("Sleep_Hours_per_Night") / 8) +
        0.25 * (col("Attendance (%)") / 100) +
        0.3 * ((10 - col("Participation_Score")) / 9) +
        0.1 * col("Extracurricular_Activities") +
        0.05 * col("Internet_Access")
    )
