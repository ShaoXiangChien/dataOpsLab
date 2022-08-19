from ETL import *
from pyspark.sql import Row, SparkSession
import pandas as pd

def test_change_col_type():
    test_data = [
        { 'col': '1' },
        { 'col': '2' },
        { 'col': '2' },
        { 'col': '3' },
        { 'col': '3' },
        { 'col': '4' }
    ]
    
    spark = SparkSession.builder.getOrCreate()
    test_df = spark.createDataFrame(map(lambda x: Row(**x), test_data))
    
    output_df = change_col_type(test_df, 'col', 'int')
    output_df_as_pd = output_df.toPandas()
    output_df_as_pd['col'] = output_df_as_pd['col'].sort_values().values
    
    assert output_df_as_pd['col'].dtype == 'int32'