# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog users

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists ryan_chynoweth;
# MAGIC use schema ryan_chynoweth; 

# COMMAND ----------

df = spark.read.format("csv").option("inferSchema", True).option("header", True).load("/databricks-datasets/bikeSharing/data-001/day.csv")
display(df)

# COMMAND ----------

# save it as a temp vieiw
df.createOrReplaceTempView("bike_data")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bike_data

# COMMAND ----------

# MAGIC %md
# MAGIC Create an external UC Table using Cluster Credentials and not an external location

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create hive table with the temp view and provide a location using abfss
# MAGIC create or replace table bike_sharing_daily_data 
# MAGIC using delta 
# MAGIC location 'abfss://commondatabase@racadlsgen2.dfs.core.windows.net/bike_sharing_daily_data_uc'
# MAGIC as 
# MAGIC select * from bike_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bike_sharing_daily_data

# COMMAND ----------

dbutils.fs.ls("abfss://commondatabase@racadlsgen2.dfs.core.windows.net/bike_sharing_daily_data_uc") # data is stored in location

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists bike_sharing_daily_data

# COMMAND ----------

dbutils.fs.ls("abfss://commondatabase@racadlsgen2.dfs.core.windows.net/") # data is maintained after dropping the unmanaged table

# COMMAND ----------


