# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Databricks SDK for Python [Documentation](https://databricks-sdk-py.readthedocs.io/en/latest/). Automatically authenticates from a Databricks notebook. 

# COMMAND ----------

BaseJob(created_time=1695220065760, creator_user_name='saurabh.shukla@databricks.com', job_id=308026716685953, settings=JobSettings(compute=None, continuous=None, email_notifications=JobEmailNotifications(no_alert_for_skipped_runs=False, on_duration_warning_threshold_exceeded=None, on_failure=None, on_start=None, on_success=None), format=<Format.MULTI_TASK: 'MULTI_TASK'>, git_source=None, health=None, job_clusters=None, max_concurrent_runs=1, name='master job', notification_settings=None, parameters=None, queue=None, run_as=None, schedule=None, tags=None, tasks=None, timeout_seconds=0, trigger=None, webhook_notifications=None))

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


