# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Databricks SDK for Python [Documentation](https://databricks-sdk-py.readthedocs.io/en/latest/). Automatically authenticates from a Databricks notebook. 

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()


# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

all_users = w.users.list()

for u in all_users:
  user_groups = u.groups
  for g in user_groups:
    if g.display=="admins":
      print(f"-------> {u.display_name} is a Workspace Admin") 

# COMMAND ----------


