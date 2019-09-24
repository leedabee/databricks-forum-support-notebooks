-- Databricks notebook source
select "hello"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # This is demonstrating the use of 'magic' commands in databricks, allowing you to switch between languages in one notebook. Since you created the  notebook as SQL, that's the default language for a block. You override using the % syntax
-- MAGIC 
-- MAGIC print('hello')

-- COMMAND ----------

-- We're back in SQL, for now
-- Below let's see the filesystem root, hopefully you see your ADLS mount: /mnt/ADLS/files/. Check out the docs for dbutils to see what else you can do at the filesystem level

-- COMMAND ----------

-- MAGIC %fs ls 

-- COMMAND ----------

-- DROP TABLE IF exists all_export_results;
CREATE TABLE all_export_results
(RECORD_NUMBER                      STRING,
 APPROVED_BY                        STRING,
 APPROVED_ON                        STRING)
USING CSV
OPTIONS (delimiter ',', header 'true')
LOCATION '/mnt/ADLS/files/all_export_results'

-- COMMAND ----------

SELECT * FROM all_export_results

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Now let's expose this table via python/pyspark DataFrame API
-- MAGIC 
-- MAGIC df = sqlContext.table('all_export_results')
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Note that you can ALSO interact with the table you created by using the SparkContext and an inline sql query
-- MAGIC 
-- MAGIC display(spark.sql("SELECT * from all_export_results"))

-- COMMAND ----------

-- MAGIC %scala
-- MAGIC // same thing in scala
-- MAGIC 
-- MAGIC display(spark.sql("SELECT * from all_export_results"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # and then use string functions to do some cleaning like you need, for example
-- MAGIC 
-- MAGIC display(spark.sql("""
-- MAGIC   SELECT REPLACE(APPROVED_BY, '#','') AS APPROVED_BY_CLEAN 
-- MAGIC   from all_export_results
-- MAGIC   """))

-- COMMAND ----------

-- So now let's clean the table you created via Spark SQL, using pyspark Dataframes

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.functions import regexp_replace, col
-- MAGIC 
-- MAGIC df = sqlContext.table('all_export_results')
-- MAGIC clean_df = df.select(*(regexp_replace(col(c),'#','').alias(c) for c in df.columns))
-- MAGIC display(clean_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC # We do need to register a new table in SparkContext if you want to work with it in SQL. That is,
-- MAGIC sqlContext.registerDataFrameAsTable(clean_df, 'all_export_results_clean')

-- COMMAND ----------

SELECT * from all_export_results_clean

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # This would be a more elegant overall solution
-- MAGIC #   1. read data using DataFrameReader (vs. using the create table ... location statement)
-- MAGIC #      this allows you to leverage the inferSchema and header options 
-- MAGIC #      and never have to spell out the schema of each of your data files 
-- MAGIC #   2. transform the data with DataFrames, allowing you to use that star expansion
-- MAGIC #   3. save the dataframe 
-- MAGIC #   4. if you have ~100 data files, you can easily wrap a for loop around all of this
-- MAGIC #       .. I would point you to python docs for basic file IO and looping
-- MAGIC 
-- MAGIC from pyspark.sql.functions import regexp_replace, col
-- MAGIC 
-- MAGIC t_df = spark.read.csv('/mnt/ADLS/files/all_export_results', header=True, inferSchema=True)
-- MAGIC t_clean_df = t_df.select(*(regexp_replace(col(c),'#','').alias(c) for c in t_df.columns))
-- MAGIC sqlContext.registerDataFrameAsTable(t_clean_df, 't_all_export_results')
-- MAGIC display(spark.sql("SELECT * FROM t_all_export_results"))
