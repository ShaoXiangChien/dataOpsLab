# A hack for us to import functions in 'ETL'
import os, sys
curr = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(curr)
sys.path.append(parent)

from ETL import change_col_type
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
    
    expected_output_df = pd.DataFrame({
        'col': [1, 2, 2, 3, 3, 4],
    })
    
    pd.testing.assert_frame_equal(left=expected_output_df, right=output_df_as_pd, check_exact=True)