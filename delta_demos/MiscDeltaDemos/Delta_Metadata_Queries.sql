-- Databricks notebook source
-- MAGIC %md
-- MAGIC 
-- MAGIC This notebook shows the various options to query metadata about Delta tables that are registered in the Hive metastore using Spark SQL. 
-- MAGIC 
-- MAGIC Please note that you will need to change the database and the table i.e. "rac_demo_db" and "test_dataset"

-- COMMAND ----------

show databases -- list databases

-- COMMAND ----------

USE rac_demo_db -- change to use the database of interest! 

-- COMMAND ----------

show tables -- simply list tables

-- COMMAND ----------

create table if not exists test_dataset (
id integer, 
text string
)

-- COMMAND ----------

insert into test_dataset values(1, "row one");
insert into test_dataset values(2, "row two");
insert into test_dataset values(3, "row three");
insert into test_dataset values(4, "row four");
insert into test_dataset values(5, "row five");

-- COMMAND ----------

describe test_dataset

-- COMMAND ----------

describe detail test_dataset

-- COMMAND ----------

describe extended test_dataset

-- COMMAND ----------

describe history test_dataset

-- COMMAND ----------

-- MAGIC %python
-- MAGIC for table in spark.catalog.listTables(): # list all tables in current database
-- MAGIC     for column in spark.catalog.listColumns(table.name): # list all columns in current table
-- MAGIC         if column.name == 'text': # provide column name here
-- MAGIC             print('Found column {} in table {}'.format(column.name, table.name))

-- COMMAND ----------


