# Databricks notebook source
# MAGIC %md
# MAGIC # Analyze Job Defintions 
# MAGIC
# MAGIC Resource: https://docs.databricks.com/api-explorer/workspace/jobs/list
# MAGIC
# MAGIC This notebook pulls all jobs from the Jobs API and looks for jobs using interactive clusters and for jobs that are not using Git. 

# COMMAND ----------

import requests
import json

# COMMAND ----------

database = ''
pat = dbutils.secrets.get('<scope>', '<token key>')
list_endpoint = "/api/2.1/jobs/list"
workspace_url = "<url i.e. https://adb-123456789012345.67.azuredatabricks.net>"

# COMMAND ----------

spark.sql(f'use {database}')

# COMMAND ----------

dbricks_auth = {"Authorization": "Bearer {}".format(pat)}

# COMMAND ----------

def call_list_api(limit=25, offset=0):
    payload = {
      'limit': limit,
      'offset': offset,
      'expand_tasks': True 
    }

    response = requests.get(f"{workspace_url}{list_endpoint}", headers=dbricks_auth, json=payload)
    return json.loads(response.content.decode('utf-8')).get('jobs')

# COMMAND ----------

offset = 0
data = call_list_api()
df = spark.createDataFrame(data).select('created_time', 'creator_user_name', 'job_id', 'settings')
display(df)

# COMMAND ----------


while True:
  offset += 25
  print(f"-----------> {offset}")
  data = call_list_api(offset=offset)
  df = df.union(spark.createDataFrame(data).select('created_time', 'creator_user_name', 'job_id', 'settings'))
  if len(data)<25:
    break 

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('job_list_raw')

# COMMAND ----------

# DBTITLE 1,Jobs Not Using Git
# MAGIC %sql
# MAGIC with task_explode as (
# MAGIC select job_id
# MAGIC , creator_user_name
# MAGIC , created_time
# MAGIC , from_unixtime(created_time/1000) as created_datetime
# MAGIC , settings.name as job_name
# MAGIC , explode(split(substring(settings.tasks, 3, len(settings.tasks)-4), "}, \\{")) as task
# MAGIC
# MAGIC from job_list_raw 
# MAGIC ),
# MAGIC task_paths as (
# MAGIC select job_id
# MAGIC , creator_user_name
# MAGIC , created_time
# MAGIC , created_datetime
# MAGIC , job_name
# MAGIC , task -- 2368
# MAGIC , regexp_extract(task, "path=([^,}]+)", 1) as path
# MAGIC
# MAGIC
# MAGIC from task_explode 
# MAGIC where contains(regexp_extract(task, "path=([^,}]+)", 1), 'Repos') = FALSE and regexp_extract(task, "path=([^,}]+)", 1) is not null and regexp_extract(task, "path=([^,}]+)", 1) <> ''
# MAGIC )
# MAGIC
# MAGIC
# MAGIC select distinct 
# MAGIC job_id
# MAGIC , creator_user_name
# MAGIC , created_datetime
# MAGIC , job_name
# MAGIC  from task_paths 

# COMMAND ----------

# DBTITLE 1,Jobs on Interactive Clusters
# MAGIC %sql
# MAGIC select job_id
# MAGIC , creator_user_name
# MAGIC , created_time
# MAGIC , from_unixtime(created_time/1000) as created_datetime
# MAGIC -- , settings
# MAGIC , settings.name as job_name
# MAGIC , settings.job_clusters 
# MAGIC , contains(settings.tasks, "existing_cluster_id") as interactive_cluster_flag
# MAGIC
# MAGIC
# MAGIC from job_list_raw 

# COMMAND ----------


