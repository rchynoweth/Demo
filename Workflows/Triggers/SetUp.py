# Databricks notebook source
# MAGIC %md
# MAGIC # Publish Raw Data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up

# COMMAND ----------

import uuid
from pyspark.sql.functions import *

# COMMAND ----------

# widget parameters 
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("storage_credential_name", "")
dbutils.widgets.text("external_location_name", "")
dbutils.widgets.text("aws_s3_url", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
storage_credential_name = dbutils.widgets.get("storage_credential_name")
external_location_name = dbutils.widgets.get("external_location_name")
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
# MAGIC ## Create External Location

# COMMAND ----------

spark.sql("""
CREATE EXTERNAL LOCATION IF NOT EXISTS {}
URL '{}'
WITH (STORAGE CREDENTIAL `{}`);
""".format(external_location_name, aws_s3_url, storage_credential_name))

# COMMAND ----------

display(spark.sql(f"describe external location {external_location_name}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish Files to External Location

# COMMAND ----------

def copy_file():
    dbutils.fs.cp(input_data_path, f"{aws_s3_url}/iot/{str(uuid.uuid4())}+_iot.json")

# COMMAND ----------

copy_file()
