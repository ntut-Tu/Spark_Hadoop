def clean_column_names(df):
    cleaned_columns = [col.replace(" ", "_")
                          .replace("(", "")
                          .replace(")", "")
                          .replace("%", "percent")
                       for col in df.columns]
    return df.toDF(*cleaned_columns)
