-- Databricks notebook source
drop table if exists mrg1;
drop table if exists mrg2;

create table mrg1(key int, id int, age int) using delta;
create table mrg2(id int, age int) using delta;

insert into mrg1 values (10,10,10);
insert into mrg2 values (10,10);
insert into mrg2 values (20,20);

merge into mrg1 using mrg2 on mrg1.id=mrg2.id
when matched then update set mrg1.age = mrg2.age
when not matched then insert(key, id, age) values(row_number() over(order by mrg2.id), mrg2.id, mrg2.age);

-- COMMAND ----------

select * from mrg1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC The above is undesired behavior. This is expected however, as the row_number will be assigned to all rows of the specified window (here, all of mrg2, sorted by id) before performing the matching logic 

-- COMMAND ----------

/* One workaround would be to extract the merge logic into a merge + an insert of the differences */
drop table if exists mrg1;
drop table if exists mrg2;

create table mrg1(key int, id int, age int) using delta;
create table mrg2(id int, age int) using delta;

insert into mrg1 values (10,10,10);
insert into mrg2 values (10,10);
insert into mrg2 values (20,20);

merge into mrg1 using mrg2 on mrg1.id=mrg2.id
when matched then update set mrg1.age = mrg2.age;

insert into mrg1 select * from 
(select row_number() over (order by id) as row_num, id, age
from mrg2 where id not in (select distinct id from mrg1));


-- COMMAND ----------

select * from mrg1;

-- COMMAND ----------

/* Or you can achieve the same without a merge statement at all */
drop table if exists mrg1;
drop table if exists mrg2;
drop table if exists merged;

create table mrg1(key int, id int, age int) using delta;
create table mrg2(id int, age int) using delta;

insert into mrg1 values (10,10,10);
insert into mrg2 values (10,10);
insert into mrg2 values (20,20);

create table merged 
as 
select mrg1.key, mrg1.id, mrg2.age
from mrg1 join mrg2 on mrg1.id = mrg2.id
union
select row_number() over (order by id) as row_num, id, age
from mrg2 where id not in (select distinct id from mrg1);


-- COMMAND ----------

select * from merged

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Side note: This is a great article explaining some ways to add sequences in spark, with a traditional SQL background assumption. There are some great points made about performance and partitioning that are worth taking the time to understand. 
-- MAGIC 
-- MAGIC <https://towardsdatascience.com/adding-sequential-ids-to-a-spark-dataframe-fa0df5566ff6>
-- MAGIC 
-- MAGIC And remember you can always work with your spark SQL table in pyspark or scala spark as a dataframe by using `df = spark.table('mrg1')`
