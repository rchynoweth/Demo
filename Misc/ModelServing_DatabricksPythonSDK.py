# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Databricks SDK for Python [Documentation](https://databricks-sdk-py.readthedocs.io/en/latest/). Automatically authenticates from a Databricks notebook. 

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import time

w = WorkspaceClient()


# COMMAND ----------

serving_endpoints = w.serving_endpoints.list()

# COMMAND ----------

for se in serving_endpoints:
  endpoint_name = se.name
  print(f"Checking Endpoint {endpoint_name}")
  endpoint = w.serving_endpoints.get(endpoint_name)
  if endpoint.config is not None:
    served_entities = endpoint.config.served_entities 
    for entity in served_entities:
      msg = entity.state.deployment_state_message if entity.state is not None else None
      zero_state = entity.scale_to_zero_enabled if entity.scale_to_zero_enabled is not None else None
      entity_version = entity.entity_version
      if msg is None or zero_state is None:
        continue 
      if msg != "Scaled to zero" or zero_state == False:
        print(f"ALERT!!! --- Endpoint Name: {endpoint_name} | Version: {entity_version} | State Message: {msg} | Scales to Zero: {zero_state}")
