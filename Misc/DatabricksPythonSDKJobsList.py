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


for j in w.jobs.list():
  job_id = j.job_id 
  owner = j.creator_user_name
  clusters = j.settings.job_clusters
  if job_id == 966078611776935:
    # print(f"{job_id} | {owner} | {clusters}")
    print(j)
    break


# COMMAND ----------

w.jobs.get(966078611776935)

# COMMAND ----------

instance_pool = w.instance_pools.get(pool_id)
instance_pool

# COMMAND ----------

w.instance_pools.edit(instance_pool_id=pool_id, max_capacity=5, min_idle_instances=0, instance_pool_name='rac_pool', node_type_id='Standard_DS3_v2')

# COMMAND ----------

instance_pool = w.instance_pools.get(pool_id)
instance_pool

# COMMAND ----------


