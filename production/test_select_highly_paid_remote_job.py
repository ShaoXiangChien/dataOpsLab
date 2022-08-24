from ETL import select_highly_paid_remote_job
from pyspark.sql import SparkSession
import pandas as pd

def test_select_highly_paid_remote_job():
    spark = SparkSession.builder.getOrCreate()
    data_path = './production/dataset/test_data.csv'
    test_df = spark.read.csv(data_path, header=True, inferSchema=True)
    expected = 18

    output_df = select_highly_paid_remote_job(test_df)
    output_df_as_pd = output_df.toPandas()

    output = output_df_as_pd.shape[0]
    
    assert output == expected
    
test_select_highly_paid_remote_job()