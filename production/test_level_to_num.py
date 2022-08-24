from ETL import level_to_num
from pyspark.sql import SparkSession
from pyspark.sql.functions import count_distinct
import pandas as pd

def test_level_to_num():
    spark = SparkSession.builder.getOrCreate()
    data_path = './dataOps/dataset/test_data.csv'
    test_df = spark.read.csv(data_path, header=True, inferSchema=True)
    expected = [27, 44, 24, 5]

    output_df = level_to_num(test_df)
    output_df_as_pd = output_df.toPandas()
    
    output = output_df_as_pd['experience_level'].value_counts().sort_index().to_list()
    
    assert output == expected