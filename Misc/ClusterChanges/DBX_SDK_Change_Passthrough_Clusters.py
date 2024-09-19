# Databricks notebook source
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Databricks SDK for Python [Documentation](https://databricks-sdk-py.readthedocs.io/en/latest/). Automatically authenticates from a Databricks notebook. 

# COMMAND ----------

import requests
import json

# COMMAND ----------

dbutils.widgets.text("WorkspaceURL", "i.e. https://adb-<workspace id>.<##>.azuredatabricks.net/")
dbutils.widgets.text("WorkspaceID", "")
dbutils.widgets.text("Token", "")
url = dbutils.widgets.get("WorkspaceURL")
workspace_id = dbutils.widgets.get("WorkspaceID")
dbtoken = dbutils.widgets.get("Token")

# COMMAND ----------

# MAGIC %md
# MAGIC # Get all clusters of interest and save to a table. 

# COMMAND ----------

df = spark.sql(f"""

select usage_metadata.cluster_id, * 
from system.billing.usage
where contains(sku_name, 'ALL_PURPOSE')
""")

display(df)

# COMMAND ----------

clusters_df = spark.sql(f"""
          with cte as (
            select * --cluster_id, cluster_name, workspace_id, change_time, delete_time
            , ROW_NUMBER() OVER (PARTITION BY cluster_id, workspace_id ORDER BY change_time DESC) AS rn

            from system.compute.clusters
            where left(cluster_name,3) not in ('job', 'dlt') and left(cluster_name,12) != 'mlflow-model'
              and delete_time is null
              and workspace_id = '{workspace_id}'
          )
          select * --cluster_id, cluster_name, workspace_id, change_time, delete_time
          from cte
          where rn = 1
            """)

display(clusters_df)

# COMMAND ----------

clusters_df.count()

# COMMAND ----------

def list_clusters(page_token, page_size=50):
  api_endpoint = "/api/2.1/clusters/list"
  auth = {"Authorization": "Bearer {}".format(dbtoken)}
  payload = {
    "page_size": page_size,
    "page_token": page_token,
  }

  response = requests.get(f"{url}{api_endpoint}", json=payload, headers=auth)
  return json.loads(response.content.decode('utf-8'))
  



# COMMAND ----------

spark.sql(""" 
CREATE TABLE IF NOT EXISTS  clusters_to_edit (
  cluster_name STRING,
  security_mode STRING,
  cluster_owner STRING,
  cluster_id STRING
)
""")

# COMMAND ----------

clusters_to_edit = []
page_token = None 
while page_token != "":
  cluster_list = list_clusters(page_token)
  page_token = cluster_list.get("next_page_token")
  clusters = cluster_list.get('clusters')
  if clusters is None:
    continue 
  for c in clusters:
    if c.get('data_security_mode') in ['LEGACY_PASSTHROUGH', 'LEGACY_SINGLE_USER', 'LEGACY_SINGLE_USER_STANDARD', 'LEGACY_TABLE_ACL']:
      row = {'cluster_name': c.get('cluster_name'), 
              'security_mode': c.get('data_security_mode'), 
              'cluster_owner': c.get('cluster_owner'), 
              'cluster_id': c.get('cluster_id'),
            }
      clusters_to_edit.append(row)
      print(f"{c.get('cluster_name')} | {c.get('data_security_mode')} | {c.get('cluster_id')}")
  print(f"--- Moving to the next page. Total Clusters Found: {len(clusters_to_edit)}. Page Token: {page_token}")

# COMMAND ----------

from pyspark.sql import Row

# Convert the list of dictionaries to a list of Rows
rows = [Row(**cluster) for cluster in clusters_to_edit]

# Create a Spark DataFrame from the list of Rows
df = spark.createDataFrame(rows)

# Save the DataFrame to a table
df.write.mode('append').saveAsTable("clusters_to_edit")

# Display the DataFrame
display(df)

# COMMAND ----------

display(spark.sql("SELECT * FROM clusters_to_edit")

# COMMAND ----------

# MAGIC %md
# MAGIC # Iterate through the table, get the current cluster configuration, change to legacy table acls, and make the change
