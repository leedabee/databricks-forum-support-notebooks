# Databricks notebook source
# Create a test Dataframe
mock_data = [('TYCO', '1303'),('EMC', '120989'), ('VOLVO', '1023295'),('BMW', '130157'),('FORD', '004')]
df = spark.createDataFrame(mock_data, ['col1', 'col2'])
display(df)

# COMMAND ----------

from pyspark.sql.functions import length
df2 = df.withColumn('length_col2', length(df.col2))
display(df2)
