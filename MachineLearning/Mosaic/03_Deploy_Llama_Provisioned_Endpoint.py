# Databricks notebook source
# MAGIC %md ##Introduction
# MAGIC This notebook deploys the `meta_llama_v3_1_70b_instruct` as a provisioned API on Databricks. 

# COMMAND ----------

dbutils.widgets.text('catalog_name', '')
dbutils.widgets.text('schema_name', '')

catalog_name = dbutils.widgets.get('catalog_name')
schema_name = dbutils.widgets.get('schema_name')

# COMMAND ----------

# create and use UC objects
spark.sql(f"create catalog if not exists {catalog_name}")
spark.sql(f"use catalog {catalog_name}")
spark.sql(f"create schema if not exists {schema_name}")
spark.sql(f"use schema {schema_name}")

# COMMAND ----------

from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")

# COMMAND ----------

model_name = "meta_llama_v3_1_70b_instruct"
model_version = '3'
full_model_name = f"system.ai.{model_name}"
model_serving_endpoint_name = "rac_llama_model_endpoint"

# COMMAND ----------

try: 
  client.get_endpoint(endpoint=model_serving_endpoint_name)
  endpoint_exists = True
  print("Model Already Exists. We will update.")
except:
  print("Model Does Not Exists.")
  endpoint_exists = False

# COMMAND ----------

endpoint_config = {
          "served_entities": [
              {
                  "name": f"{model_name}-{model_version}",
                  "entity_name": full_model_name,
                  "entity_version": model_version,
                  "workload_type": "GPU_SMALL",
                  "scale_to_zero_enabled": False,
                  "min_provisioned_throughput": 6000,
                  "max_provisioned_throughput": 12000,
              }
          ],
          "auto_capture_config": {
              "catalog_name": catalog_name,
              "schema_name": schema_name,
              "table_name_prefix": model_name,
          },
          "traffic_config": {
              "routes": [
                  {
                      "served_model_name": f"{model_name}-{model_version}",
                      "traffic_percentage": 100,
                  }
              ]
          },
      }

# COMMAND ----------

if endpoint_exists == False:
  print("Creating Endpoint.")
  endpoint = client.create_endpoint(
      name=model_serving_endpoint_name,
      config=endpoint_config,
  )
else :
  print("Updating Endpoint.")
  del endpoint_config['auto_capture_config']
  endpoint = client.update_endpoint(
    endpoint=model_serving_endpoint_name,
    config=endpoint_config,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC To test the API you can paste the following into the UI test console in Databricks. 
# MAGIC
# MAGIC ```
# MAGIC {
# MAGIC   "dataframe_records": [
# MAGIC   {
# MAGIC     "content": "Embed this sentence as fast as possible. why is this only returning the number one."
# MAGIC   }
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC Output: 
# MAGIC ```
# MAGIC {
# MAGIC   "predictions": [
# MAGIC     0.043806903064250946,
# MAGIC     -0.006325399503111839,
# MAGIC     0.08014420419931412,
# MAGIC     ...
# MAGIC     ...
# MAGIC     -0.03335569053888321
# MAGIC   ]
# MAGIC }
# MAGIC ```
# MAGIC
# MAGIC It appears that when pinging the endpoint upon index creation we send several formats to determine which is the one to use. These were the 5 most recent requests from my last attempt: 
# MAGIC ```
# MAGIC {"dataframe_split":{"data":[["Welcome to databricks vector search"]],"columns":[0],"index":[0]}}	{"error_code": "BAD_REQUEST", "message": "Model is missing inputs ['content']. Note that there were extra inputs: [0]"}
# MAGIC {"dataframe_split":{"data":[["Welcome to databricks vector search"]],"columns":[0],"index":[0]}}	{"error_code": "BAD_REQUEST", "message": "Model is missing inputs ['content']. Note that there were extra inputs: [0]"}
# MAGIC {"input":["Vector Search probe query for endpoint type: EXTERNAL_MODEL"]}	{"error_code": "BAD_REQUEST", "message": "Model is missing inputs ['content']. Note that there were extra inputs: ['input']"}
# MAGIC {"input":["Vector Search probe query for endpoint type: FOUNDATION_MODEL_API"]}	{"error_code": "BAD_REQUEST", "message": "Model is missing inputs ['content']. Note that there were extra inputs: ['input']"}
# MAGIC {"dataframe_split":{"data":[["Vector Search probe query for endpoint type: MODELS"]],"columns":[0],"index":[0]}}	{"error_code": "BAD_REQUEST", "message": "Model is missing inputs ['content']. Note that there were extra inputs: [0]"}
# MAGIC ```
