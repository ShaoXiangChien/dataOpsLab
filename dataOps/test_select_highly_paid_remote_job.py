from ETL import select_highly_paid_remote_job
from pyspark.sql import SparkSession
import pandas as pd

def test_select_highly_paid_remote_job():
    spark = SparkSession.builder.getOrCreate()
    data_path = '/Workspace/Repos/a-ericchien@microsoft.com/dataOpsLab/dataOps/dataset/test_data.csv'
    test_df = spark.read.csv(f'file://{ data_path }', header=True, inferSchema=True)
    expected = 12

    output_df = select_highly_paid_remote_job()
    output_df_as_pd = output_df.toPandas()

    output = output_df_as_pd.count()
    
    assert output == expected