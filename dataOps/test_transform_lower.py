from ETL import *
from pyspark.sql import Row, SparkSession
import pandas as pd

def test_transform_lower():
    test_data = [
        { 'col': 'APPLE' },
        { 'col': 'Apple' },
        { 'col': 'aPPle' },
        { 'col': 'aPpLe' },
        { 'col': 'ApPlE' },
        { 'col': 'apple' }
    ]
    
    spark = SparkSession.builder.getOrCreate()
    test_df = spark.createDataFrame(map(lambda x: Row(**x), test_data))
    
    output_df = transform_lower(test_df, 'col')
    output_df_as_pd = output_df.toPandas()
    
    expected_output_df = pd.DataFrame({
        'col': ['apple', 'apple', 'apple', 'apple', 'apple', 'apple'],
    })
    
    pd.testing.assert_frame_equal(left=expected_output_df, right=output_df_as_pd, check_exact=True)