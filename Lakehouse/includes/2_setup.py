# Databricks notebook source
# Get the caller's team name, which was passed in as a parameter by the notebook that called me.
# We'll strip special characters from this field, then use it to define unique path names (local and dbfs) as well as a unique database name.
# This prevents name collisions in a multi-team flight school.
dbutils.widgets.text("database_name", "rac_demo_db");
dbutils.widgets.text("user_name", "ryan.chynoweth@databricks.com");

# COMMAND ----------

# import to enable removal on non-alphanumeric characters (re stands for regular expressions)
import re

# Get the email address entered by the user on the calling notebook
user_name = dbutils.widgets.get("user_name")
database_name = dbutils.widgets.get("database_name")
print(f"Data entered in database name field: {database_name}")
print(f"Data entered in user name field: {user_name}")

# Construct the unique path to be used to store files on the local file system
local_data_path = f"/tmp/downloads/{user_name}/"
print(f"Path to be used for Local Files: {local_data_path}")

# Construct the unique path to be used to store files on the DBFS file system
dbfs_data_path = f"/Users/{user_name}/"
print(f"Path to be used for DBFS Files: {dbfs_data_path}")

# Construct the unique database name
database_name = f"{database_name}"
print(f"Database Name: {database_name}")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

# Enables running shell commands from Python
# I need to do this, rather than just the %sh magic command, because I want to pass in python variables

import subprocess
from shutil import copyfile

# COMMAND ----------

# Delete local directories that may be present from a previous run

process = subprocess.Popen(['rm', '-f', '-r', local_data_path],
                     stdout=subprocess.PIPE, 
                     stderr=subprocess.PIPE)
stdout, stderr = process.communicate()

stdout.decode('utf-8'), stderr.decode('utf-8')

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

# Copy the downloaded data to DBFS

dbutils.fs.rm(f"dbfs:/FileStore/flight/{dbfs_data_path}/sensor_readings_current_labeled.csv")

dbutils.fs.mkdirs(f"/dbfs/{dbfs_data_path}bronze")

copyfile(f"{local_data_path}/sensor_readings_current_labeled_v4.csv", f"/dbfs/{dbfs_data_path}bronze/sensor_readings_current_labeled.csv")


# COMMAND ----------

dataPath = f"{dbfs_data_path}bronze/sensor_readings_current_labeled.csv"

df = spark.read\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .csv(dataPath)

display(df)

# COMMAND ----------

df.createOrReplaceTempView("input_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS current_readings_labeled;
# MAGIC 
# MAGIC CREATE TABLE current_readings_labeled 
# MAGIC USING DELTA
# MAGIC AS (
# MAGIC   SELECT * FROM input_vw
# MAGIC )

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
# MAGIC   FROM input_vw
# MAGIC )

# COMMAND ----------

# Return to the caller, passing the variables needed for file paths and database

response = local_data_path + " " + dbfs_data_path + " " + database_name

dbutils.notebook.exit(response)
