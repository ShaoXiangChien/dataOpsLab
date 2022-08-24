# Databricks notebook source
base_path = "/Workspace/Repos/a-ericchien@microsoft.com/dataOpsLab/"
path = f"{base_path}/dataOps/dataset/ds_salaries.csv"

# COMMAND ----------

df = spark.read.csv("file://" + path, header=True, inferSchema=True)
print(df)

# COMMAND ----------

from ETL import *
df = level_to_num(df)
df = select_highly_paid_remote_job(df)
display(df)

# COMMAND ----------

import os

df.coalesce(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t").option("compression", "gzip").save(f"dbfs:/tmp/cleaned_ds_salaries.csv")

# COMMAND ----------


