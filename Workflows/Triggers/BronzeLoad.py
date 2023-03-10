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

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("storage_credential_name", "")
dbutils.widgets.text("external_location_name", "")
dbutils.widgets.text("azure_storage_url", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
storage_credential_name = dbutils.widgets.get("storage_credential_name")
external_location_name = dbutils.widgets.get("external_location_name")
azure_storage_url = dbutils.widgets.get("azure_storage_url")

# COMMAND ----------

print(azure_storage_url)

# COMMAND ----------

spark.sql(f"use catalog {catalog}")
spark.sql(f"use schema {schema}")

# COMMAND ----------

input_data_path = "/databricks-datasets/iot/iot_devices.json"

# COMMAND ----------

spark.sql("""
CREATE EXTERNAL LOCATION {}
URL '{}'
WITH (STORAGE CREDENTIAL `{}`);
""".format(external_location_name, azure_storage_url, storage_credential_name))

# COMMAND ----------

df = spark.read.json(input_data_path)
display(df)

# COMMAND ----------


