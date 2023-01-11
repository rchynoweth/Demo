# Databricks notebook source
# MAGIC %md
# MAGIC # Default Values in Databricks 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Default Values with Parquet Tables 
# MAGIC 
# MAGIC Below are a number of examples using default values with parquet tables. I provide several different examples in order to show the different ways to write data to the table and still obtain the default value for columns that are not provided. 

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists default_parquet_test

# COMMAND ----------

# MAGIC %sql
# MAGIC create table default_parquet_test 
# MAGIC (val1 int, val2 int default 42)
# MAGIC using parquet;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC insert into default_parquet_test(val1, val2) VALUES(1);
# MAGIC 
# MAGIC SELECT * FROM default_parquet_test

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists default_parquet_test

# COMMAND ----------

# MAGIC %sql
# MAGIC create table default_parquet_test 
# MAGIC (val1 int, val2 int default 42)
# MAGIC using parquet;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC insert into default_parquet_test(val1) values(1);
# MAGIC 
# MAGIC select * from default_parquet_test

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists default_parquet_test

# COMMAND ----------

# MAGIC %sql
# MAGIC create table default_parquet_test 
# MAGIC (val1 int, val2 int default 42)
# MAGIC using parquet;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC insert into default_parquet_test(val1, val2) values(1, default);
# MAGIC 
# MAGIC select * from default_parquet_test

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists default_parquet_test

# COMMAND ----------

# MAGIC %md
# MAGIC ## Default Values with Delta Lake

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create or replace table default_delta_table (val1 INT, val2 INT DEFAULT 42) using delta;
# MAGIC -- select * from default_delta_table;

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Structured Streaming with Generated Columns and Default Values 

# COMMAND ----------


