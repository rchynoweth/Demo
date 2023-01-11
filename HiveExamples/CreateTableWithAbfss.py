# Databricks notebook source
spark.conf.set("fs.azure.account.key.racadlsgen2.dfs.core.windows.net", dbutils.secrets.get("rac_scope", "adls_key"))

dbutils.fs.ls("abfss://commondatabase@racadlsgen2.dfs.core.windows.net/")



# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists rac_demo_db

# COMMAND ----------

# MAGIC %sql
# MAGIC use rac_demo_db

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

# MAGIC %sql
# MAGIC -- create hive table with the temp view and provide a location using abfss
# MAGIC create table if not exists bike_sharing_daily_data 
# MAGIC using delta 
# MAGIC location 'abfss://commondatabase@racadlsgen2.dfs.core.windows.net/bike_sharing_daily_data'
# MAGIC as 
# MAGIC select * from bike_data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bike_sharing_daily_data

# COMMAND ----------

dbutils.fs.ls("abfss://commondatabase@racadlsgen2.dfs.core.windows.net/") # data is stored in location

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists bike_sharing_daily_data

# COMMAND ----------

dbutils.fs.ls("abfss://commondatabase@racadlsgen2.dfs.core.windows.net/") # data is maintained after dropping the unmanaged table

# COMMAND ----------


