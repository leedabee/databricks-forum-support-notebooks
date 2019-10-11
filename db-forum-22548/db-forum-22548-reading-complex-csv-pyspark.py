# Databricks notebook source
# MAGIC %fs
# MAGIC head '/FileStore/tables/mock_data.csv'

# COMMAND ----------

# MAGIC %fs
# MAGIC head '/FileStore/tables/mock_data_preferred.csv'

# COMMAND ----------

# the default behavior is that any delimiters found within double quotes will be escaped
df = spark.read.csv('/FileStore/tables/mock_data_preferred.csv', header=True)
display(df)


# COMMAND ----------

from pyspark.sql.types import StringType

# Demonstrate custom function using indexing and a schema assumption
# This assumes that there are 3 columns and AUTHOR_LIST will be the 2nd column

def processRow(row):
    splits = row.split(',')
    result = [splits[0], ', '.join(str(x) for x in splits[1:-1]), splits[-1]]
    return result
  
data = sc.textFile('/FileStore/tables/mock_data.csv')
header = data.first() #extract header
filtered = data.filter(lambda row: row != header) # get just the data
df = filtered.map(lambda row: processRow(row)).toDF(header.split(','))

display(df)

# COMMAND ----------


