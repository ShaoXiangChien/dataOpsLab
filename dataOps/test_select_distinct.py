from ETL import *
from pyspark.sql import Row, SparkSession
import pandas as pd

def test_select_distinct():
    test_data = [
        { 'col': 1 },
        { 'col': 1 },
        { 'col': 2 },
        { 'col': 3 },
        { 'col': 4 },
        { 'col': 4 },
        { 'col': 5 }
    ]

    spark = SparkSession.builder.getOrCreate()
    test_df = spark.createDataFrame(map(lambda x: Row(**x), test_data))

    output_df = select_distinct(test_df, 'col >= 1')
    output_df_as_pd = output_df.toPandas()
    output_df_as_pd['col'] = output_df_as_pd['col'].sort_values().values

    expected_output_df = pd.DataFrame({
        'col': [1, 2, 3, 4, 5],
    })

    pd.testing.assert_frame_equal(left=expected_output_df, right=output_df_as_pd, check_exact=True)