from pyspark.sql.functions import lower,col

def select_distinct(df, cond):
    return df.where(cond).distinct()

def transform_lower(df, column):
    return df.withColumn(column, lower(col(column)))

def change_col_type(df, column, dtype):
    return df.withColumn(column, col(column).cast(dtype))