# Databricks notebook source
# MAGIC %md
# MAGIC # Load Bronze Data 
# MAGIC 
# MAGIC The purpose of this notebook is to demonstrate the triggers available in [Databricks Workflows](https://docs.databricks.com/workflows). 
# MAGIC 
# MAGIC - [Continuous Job](https://docs.databricks.com/workflows/jobs/jobs.html#run-a-continuous-job)  
# MAGIC - [File Arrival Job](https://docs.databricks.com/workflows/jobs/jobs.html#run-a-job-when-new-files-arrive)
# MAGIC 
# MAGIC We will use this notebook to configure an external location to load data from. Please note that we will use the Databricks provided datasets (`/databricks-datasets`) throughout this example and copy the data to the external location which will be particularly useful for file arrival triggers. 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC We will use Databricks Unity Catalog as it is required for file Triggers. 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# widget parameters 
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("aws_s3_url", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
aws_s3_url = dbutils.widgets.get("aws_s3_url")

# variables 
input_data_path = "/databricks-datasets/iot/iot_devices.json"

# COMMAND ----------

spark.sql(f"create catalog if not exists {catalog}")
spark.sql(f"use catalog {catalog}")
spark.sql(f"create schema if not exists {schema}")
spark.sql(f"use schema {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Table with Auto Loader

# COMMAND ----------

schema_path = f"/tmp/{catalog}/bronze_schema"
checkpoint_path = f"/tmp/{catalog}/bronze_ckpt"
file_path = f"{aws_s3_url}/iot/*.json"

# COMMAND ----------

# Configure Auto Loader to ingest JSON data to a Delta table
streaming_query = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", schema_path)
  .load(file_path)
  .select("*", input_file_name().alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable("bronze_iot_demo"))

streaming_query.awaitTermination() # wait for stream to complete before continuing 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_iot_demo

# COMMAND ----------


