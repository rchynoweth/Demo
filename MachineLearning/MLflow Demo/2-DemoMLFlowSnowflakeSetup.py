# Databricks notebook source
dbutils.widgets.text("database_name", "rac_demo_db");

# COMMAND ----------

database_name = dbutils.widgets.get("database_name")

# COMMAND ----------

dbutils.fs.ls("/Users/ryan.chynoweth@databricks.com")

# COMMAND ----------

# Construct the unique path to be used to store files on the local file system
local_data_path = "/dbfs/Users/ryan.chynoweth@databricks.com/downloads"
print(f"Path to be used for Local Files: {local_data_path}")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

import subprocess

# COMMAND ----------

dbutils.fs.rm(local_data_path[5:], True)

# COMMAND ----------

# Create local directories used in the workshop

process = subprocess.Popen(['mkdir', '-p', local_data_path],
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE)

stdout, stderr = process.communicate()

stdout.decode('utf-8'), stderr.decode('utf-8')

# COMMAND ----------

# Download Initial CSV file used in the workshop

process = subprocess.Popen(['wget', '-P', local_data_path, 'https://www.dropbox.com/s/vuaq3vkbzv8fgml/sensor_readings_current_labeled_v4.csv'],
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE)

stdout, stderr = process.communicate()

stdout.decode('utf-8'), stderr.decode('utf-8')

# COMMAND ----------

dbutils.fs.ls(local_data_path[5:])

# COMMAND ----------

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(local_data_path[5:])

display(df)

# COMMAND ----------

df.createOrReplaceTempView("mlflow_input_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS current_readings_labeled;
# MAGIC 
# MAGIC CREATE TABLE current_readings_labeled 
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT * FROM mlflow_input_vw
# MAGIC )

# COMMAND ----------

# Use secrets DBUtil to get Snowflake credentials.
user = dbutils.secrets.get("demo", "rac_snowflake_user")
password = dbutils.secrets.get("demo", "rac_snowflake_password")
url = dbutils.secrets.get("demo", "rac_snowflake_url")

# snowflake connection options
options = {
  "sfUrl":  url,
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": "DEMO_DB",
  "sfSchema": "PUBLIC",
  "sfWarehouse": "DEMO_CLUSTER"
}

# COMMAND ----------

df = spark.sql("select * from mlflow_input_vw")

(df.write
  .format("snowflake")
  .mode("overwrite")
  .options(**options)
  .option("dbtable", "rac_mlflow_current_readings_labeled")
  .save())

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS current_readings_unlabeled;
# MAGIC 
# MAGIC CREATE TABLE current_readings_unlabeled 
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     reading_time,
# MAGIC     device_type,
# MAGIC     device_id,
# MAGIC     reading_1,
# MAGIC     reading_2,
# MAGIC     reading_3
# MAGIC   FROM mlflow_input_vw
# MAGIC )

# COMMAND ----------

df = spark.sql("select * from current_readings_unlabeled")

(df.write
  .format("snowflake")
  .mode("overwrite")
  .options(**options)
  .option("dbtable", "rac_mlflow_current_readings_unlabeled")
  .save())

# COMMAND ----------

dbutils.fs.ls("/Users/ryan.chynoweth@databricks.com/downloads")
