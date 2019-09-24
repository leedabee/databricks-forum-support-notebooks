# Databricks notebook source
# Create a test Dataframe
mock_data = [('Bob', 't'),('Sue', 'f'), ('Paul', 't'),('Alice', '')]
df = spark.createDataFrame(mock_data, ['Name', 'Bool_String'])
display(df)

# COMMAND ----------

# Demonstrate udf usage with just pyspark
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import udf

def tf(x):
    if x == 't': return True
    elif x == 'f': return False
    else: return None
tf_udf = udf(tf, BooleanType())

df2 = df.withColumn('Bool_Value', tf_udf(df.Bool_String))
display(df2)

# COMMAND ----------

# Demonstrate udf usage with Spark SQL
from pyspark.sql.types import BooleanType

def tf(x):
    if x == 't': return True
    elif x == 'f': return False
    else: return None
    
spark.udf.register("tf_udf", tf, BooleanType())

df.registerTempTable('temp')

df3 = spark.sql("""
   SELECT 
     Name, 
     Bool_String, 
     tf_udf(Bool_String) as Bool_Value
   FROM temp
""")

display(df3)
