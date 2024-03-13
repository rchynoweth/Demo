# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

w.workspace_conf.set_status({ "enableWorkspaceFilesystem": "true" })

# COMMAND ----------


