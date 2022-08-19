# Databricks notebook source
base_path = "/Workspace/Repos/a-ericchien@microsoft.com/dataOpsLab/"
path = "/Workspace/Repos/a-ericchien@microsoft.com/dataOpsLab/2012_SAT_Results.csv"

# COMMAND ----------

df = spark.read.csv("file://" + path, header=True, inferSchema= True)
print(df)

# COMMAND ----------

from ETL import *
df = change_col_type(df, 'SAT_test_takers_num', 'int')
print(df)

# COMMAND ----------

df = select_distinct(df, "SAT_test_takers_num < 50")
display(df)

# COMMAND ----------

df = transform_lower(df, "SCHOOL NAME")
display(df)

# COMMAND ----------

import os
df.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("compression", "gzip").save(f"file://{os.getcwd()}/cleaned_data.csv")

# COMMAND ----------


