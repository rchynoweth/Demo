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

pool_id = '0816-165607-drub6-pool-au98236n'

for p in w.instance_pools.list():
  if p.instance_pool_id == pool_id:
    print(f"{p.instance_pool_name} | {p.instance_pool_id}")

# COMMAND ----------

instance_pool = w.instance_pools.get(pool_id)
instance_pool

# COMMAND ----------

w.instance_pools.edit(instance_pool_id=pool_id, max_capacity=10, min_idle_instances=0, instance_pool_name='rac_pool', node_type_id='Standard_DS3_v2')

# COMMAND ----------

instance_pool = w.instance_pools.get(pool_id)
instance_pool

# COMMAND ----------


