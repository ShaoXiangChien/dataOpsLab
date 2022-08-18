# Databricks notebook source
from ETL import *
from pyspark.sql import Row, SparkSession
import pandas as pd

# COMMAND ----------

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
    
test_select_distinct()

# COMMAND ----------

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
    
test_transform_lower()

# COMMAND ----------

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
    
test_change_col_type()
