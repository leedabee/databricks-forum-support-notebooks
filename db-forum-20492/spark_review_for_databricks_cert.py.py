# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Developer Certification (python)
# MAGIC 
# MAGIC <https://academy.databricks.com/exam/crt020-python>
# MAGIC 
# MAGIC ## Expectation of Knowledge
# MAGIC 
# MAGIC ### Spark Architecture Components
# MAGIC 
# MAGIC Candidates are expected to be familiar with the following architectural components and their relationship to each other:
# MAGIC 
# MAGIC - Driver
# MAGIC - Executor
# MAGIC - Cores/Slots/Threads
# MAGIC - Partitions
# MAGIC 
# MAGIC ### Spark Execution
# MAGIC 
# MAGIC Candidates are expected to be familiar with Spark’s execution model and the breakdown between the different elements:
# MAGIC 
# MAGIC - Jobs
# MAGIC - Stages
# MAGIC - Tasks
# MAGIC 
# MAGIC ### Spark Concepts
# MAGIC 
# MAGIC Candidates are expected to be familiar with the following concepts:
# MAGIC 
# MAGIC - Caching
# MAGIC - Shuffling
# MAGIC - Partitioning
# MAGIC - Wide vs. Narrow Transformations
# MAGIC - DataFrame Transformations vs. Actions vs. Operations
# MAGIC - High-level Cluster Configuration
# MAGIC 
# MAGIC ### DataFrames API
# MAGIC - SparkContext
# MAGIC   - Candidates are expected to know how to use the SparkContext to control basic
# MAGIC     configuration settings such as spark.sql.shuffle.partitions.
# MAGIC 
# MAGIC - SparkSession
# MAGIC   - Candidates are expected to know how to:
# MAGIC     - Create a DataFrame/Dataset from a collection (e.g. list or set)
# MAGIC     - Create a DataFrame for a range of numbers
# MAGIC     - Access the DataFrameReaders
# MAGIC     - Register User Defined Functions (UDFs)
# MAGIC     
# MAGIC - DataFrameReader
# MAGIC   - Candidates are expected to know how to:
# MAGIC     - Read data for the "core" data formats (CSV, JSON, JDBC, ORC, Parquet, text and tables)
# MAGIC     - How to configure options for specific formats
# MAGIC     - How to read data from non-core formats using format() and load()
# MAGIC     - How to specify a DDL-formatted schema
# MAGIC     - How to construct and specify a schema using the StructType classes
# MAGIC 
# MAGIC - DataFrameWriter
# MAGIC   - Candidates are expected to know how to:
# MAGIC     - Write data to the "core" data formats (csv, json, jdbc, orc, parquet, text and tables)
# MAGIC     - Overwriting existing files
# MAGIC     - How to configure options for specific formats
# MAGIC     - How to write a data source to 1 single file or N separate files
# MAGIC     - How to write partitioned data
# MAGIC     - How to bucket data by a given set of columns
# MAGIC 
# MAGIC - DataFrame
# MAGIC   - Have a working understanding of every action such as take(), collect(), and foreach()
# MAGIC   - Have a working understanding of the various transformations and how they
# MAGIC     work such as producing a distinct set, filtering data, repartitioning and
# MAGIC     coalescing, performing joins and unions as well as producing aggregates
# MAGIC   - Know how to cache data, specifically to disk, memory or both
# MAGIC   - Know how to uncache previously cached data
# MAGIC   - Converting a DataFrame to a global or temp view.
# MAGIC   - Applying hints
# MAGIC   
# MAGIC - Row & Column
# MAGIC   - Candidates are expected to know how to work with row and columns to successfully extract data from a DataFrame
# MAGIC 
# MAGIC - Spark SQL Functions
# MAGIC   - When instructed what to do, candidates are expected to be able to employ the
# MAGIC     multitude of Spark SQL functions. Examples include, but are not limited to:
# MAGIC     - Aggregate functions: getting the first or last item from an array or computing the min and max values of a column.
# MAGIC     - Collection functions: testing if an array contains a value, exploding or flattening data.
# MAGIC     - Date time functions: parsing strings into timestamps or formatting timestamps into strings
# MAGIC     - Math functions: computing the cosign, floor or log of a number
# MAGIC     - Misc functions: converting a value to crc32, md5, sha1 or sha2
# MAGIC     - Non-aggregate functions: creating an array, testing if a column is null, not-null, nan, etc
# MAGIC     - Sorting functions: sorting data in descending order, ascending order, and sorting with proper null handling
# MAGIC     - String functions: applying a provided regular expression, trimming string and extracting substrings.
# MAGIC     - UDF functions: employing a UDF function.
# MAGIC     - Window functions: computing the rank or dense rank.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SparkContext
# MAGIC 
# MAGIC Candidates are expected to know how to use the SparkContext to control basic configuration settings such as spark.sql.shuffle.partitions.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 6)
spark.conf.set("spark.executor.memory", "2g")

# COMMAND ----------

print(spark.conf.get("spark.sql.shuffle.partitions"), ",", spark.conf.get("spark.executor.memory"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.sql.shuffle.partitions = 8;
# MAGIC SET spark.executor.memory = 4g;

# COMMAND ----------

print(spark.conf.get("spark.sql.shuffle.partitions"), ",", spark.conf.get("spark.executor.memory"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## SparkSession
# MAGIC 
# MAGIC Candidates are expected to know how to:
# MAGIC - Create a DataFrame/Dataset from a collection (e.g. list or set)

# COMMAND ----------

from pyspark.sql.types import IntegerType

list_df = spark.createDataFrame([1, 2, 3, 4], IntegerType())
display(my_list_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC - Create a DataFrame for a range of numbers

# COMMAND ----------

ints_df = spark.range(1000).toDF("number")
display(ints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC - Access the DataFrameReaders

# COMMAND ----------

df = spark.read.csv('/FileStore/tables/input.csv', inferSchema=True)
# spark.read.parquet()
# spark.read.json()
# spark.read.format().open()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC - Register User Defined Functions (UDFs)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def power3(value):
  return value ** 3

spark.udf.register("power3py", power3, IntegerType())

# COMMAND ----------

power3udf = udf(power3, IntegerType())
power3_ints_df = ints_df.select("number", power3udf("number").alias("power3"))
display(power3_ints_df)

# COMMAND ----------

spark.range(1, 20).registerTempTable("test")

# COMMAND ----------

# MAGIC %sql select id, power3py(id) as power3 from test

# COMMAND ----------

# MAGIC %md
# MAGIC ## DataFrameReader

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC   - Read data for the "core" data formats (CSV, JSON, JDBC, ORC, Parquet, text and tables)

# COMMAND ----------

data_file = "/FileStore/tables/sales.csv"

df = spark.read.csv(data_file)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC   - How to configure options for specific formats

# COMMAND ----------

df = spark.read.csv(data_file, header=True, inferSchema=True)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC   - How to read data from non-core formats using format() and load()

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema","true").option("header","true").load(data_file)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC   - How to construct and specify a schema using the StructType classes

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, LongType

myManualSchema = StructType([
  StructField("field1", StringType()),
  StructField("field2", StringType()),
  StructField("field3", StringType())
])

df3 = spark.read.format("csv").schema(myManualSchema).option("header","true").load(data_file)
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC - How to specify a DDL-formatted schema

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## DataFrameWriter

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC   - Write data to the "core" data formats (csv, json, jdbc, orc, parquet, text and tables)

# COMMAND ----------

df.write.parquet('myparquetfile')
df.write.saveAsTable('mytable')

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC   - Overwriting existing files

# COMMAND ----------

#df.write.mode("overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC - How to configure options for specific formats

# COMMAND ----------

csvFile = (spark.read.format("csv")
  .option("header", "true")
  .option("inferSchema", "true")
  .load(data_file))

csvFile.write.format("csv").mode("overwrite").option("sep", "\t").save("my-tsv-file.tsv")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC - How to write a data source to 1 single file or N separate files

# COMMAND ----------

# df.coalesce(1) 
# df.repartition(1)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC - How to write partitioned data
# MAGIC - How to bucket data by a given set of columns

# COMMAND ----------

(df
    .write
    .partitionBy("ProductKey")
    .bucketBy(42, "OrderDateKey")
    .saveAsTable("orders_partitioned_bucketed"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## DataFrame

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC - Have a working understanding of every action such as take(), collect(), and foreach()

# COMMAND ----------

# MAGIC %md
# MAGIC - Have a working understanding of the various transformations and how they work such as producing a distinct set, filtering data, repartitioning and coalescing, performing joins and unions as well as producing aggregates

# COMMAND ----------

# MAGIC %md
# MAGIC - Know how to cache data, specifically to disk, memory or both

# COMMAND ----------

from pyspark.storagelevel import StorageLevel

df.persist(StorageLevel.MEMORY_AND_DISK)

# COMMAND ----------

# MAGIC %md
# MAGIC - Know how to uncache previously cached data

# COMMAND ----------

df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC - Converting a DataFrame to a global or temp view

# COMMAND ----------

# df.createOrReplaceTempView("<table-name>")

# COMMAND ----------

# MAGIC %md
# MAGIC - Applying hints

# COMMAND ----------

# df.join(df2.hint("broadcast"), "name").show()
