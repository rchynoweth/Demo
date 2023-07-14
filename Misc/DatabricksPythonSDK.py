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

for p in w.instance_pools.list():
  print(p.instance_pool_name)

# COMMAND ----------

w.clusters.list_node_types()

# COMMAND ----------

created = w.instance_pools.create(instance_pool_name=f'sdk-{time.time_ns()}', node_type_id='r3.xlarge')

# COMMAND ----------

job_id = 327770081688124
job_name = 'test_queue'

# COMMAND ----------

job1 = w.jobs.run_now(job_id=job_id)

# COMMAND ----------

job2 = w.jobs.run_now(job_id=job_id)

# COMMAND ----------

job3 = w.jobs.run_now(job_id=job_id)

# COMMAND ----------

job1.result()

# COMMAND ----------

job2.result()

# COMMAND ----------

job3.result()

# COMMAND ----------


