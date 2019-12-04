# Databricks notebook source
# MAGIC %md
# MAGIC Please note I'm in a python notebook, making use of magic commands (e.g. `%sql`) to drop into different language blocks

# COMMAND ----------

# Create test dataframes and register them in spark sql context as tables

mock_data1 = [('Bob', 1),('Sue', 3), ('Paul', 4),('Alice', 2),('Josh', 2)]
spark.createDataFrame(mock_data1, ['colA', 'colB']).registerTempTable('t1')

mock_data2 = [('Bob', 3,'XYZ'),('Sue', 2,'XYZ'), ('Pam', 2,'XYZ'),('Arthur', 2,'XYZ'), ('Josh',1,'ABC')]
spark.createDataFrame(mock_data2, ['colA', 'colB', 'colC']).registerTempTable('t2')


# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC update t1
# MAGIC set t1.colB=CASE WHEN t2.colB>t1.colB THEN t2.colB ELSE t1.colB +t2.colB END
# MAGIC from t1
# MAGIC inner join t2 ON t1.colA=t2.colA
# MAGIC where t2.colC='XYZ'

# COMMAND ----------

# MAGIC %md
# MAGIC First instinct, use a subquery? ...

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC update t1
# MAGIC set t1.colB=
# MAGIC (select CASE WHEN t2.colB>t1.colB THEN t2.colB ELSE t1.colB +t2.colB END
# MAGIC from t1
# MAGIC inner join t2 ON t1.colA=t2.colA
# MAGIC where t2.colC='XYZ')

# COMMAND ----------

# MAGIC %md
# MAGIC Nope, doesn't work. But does remind me that updates are for delta sources only. Let's convert to delta tables

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop table if exists dt1;
# MAGIC drop table if exists dt2;
# MAGIC 
# MAGIC CREATE TABLE dt1
# MAGIC USING delta
# MAGIC AS SELECT * from t1;
# MAGIC 
# MAGIC CREATE TABLE dt2
# MAGIC USING delta
# MAGIC AS SELECT * from t2;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC update dt1
# MAGIC set dt1.colB=
# MAGIC (select CASE WHEN dt2.colB>dt1.colB THEN dt2.colB ELSE dt1.colB +dt2.colB END
# MAGIC from dt1
# MAGIC inner join dt2 ON dt1.colA=dt2.colA
# MAGIC where dt2.colC='XYZ')

# COMMAND ----------

# MAGIC %md
# MAGIC Issue with join. Let's try using the Merge statement. This seems like the best solution here, assuming we're levering the delta architecture and storage format
# MAGIC 
# MAGIC <https://docs.databricks.com/spark/latest/spark-sql/language-manual/merge-into.html>

# COMMAND ----------

# MAGIC %sql 
# MAGIC merge into dt1
# MAGIC using dt2 ON dt1.colA=dt2.colA and dt2.colB > dt1.colB and dt2.colC = 'XYZ'
# MAGIC WHEN MATCHED THEN UPDATE set colB = dt2.colB;
# MAGIC 
# MAGIC merge into dt1
# MAGIC using dt2 ON dt1.colA=dt2.colA and dt2.colB <= dt1.colB and dt2.colC = 'XYZ'
# MAGIC WHEN MATCHED THEN UPDATE set colB = dt1.colB + dt2.colB;

# COMMAND ----------

# MAGIC %md
# MAGIC This worked, but the sequential updates made colB be updated accidentally in the 2nd statement

# COMMAND ----------

# MAGIC %sql 
# MAGIC drop view if exists joined;
# MAGIC 
# MAGIC create temporary view joined as
# MAGIC select dt1.colA, CASE WHEN dt2.colB>dt1.colB THEN dt2.colB ELSE dt1.colB + dt2.colB END as colB
# MAGIC from dt1 inner join dt2 ON dt1.colA=dt2.colA
# MAGIC where dt2.colC='XYZ';
# MAGIC 
# MAGIC merge into dt1
# MAGIC using joined ON dt1.colA=joined.colA
# MAGIC WHEN MATCHED THEN UPDATE set colB = joined.colB;

# COMMAND ----------

# MAGIC %md 
# MAGIC Success!

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dt1
