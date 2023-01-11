# Databricks notebook source
import requests 
import json
import datetime
import time 
import pandas as pd
import random

# COMMAND ----------

pat = dbutils.secrets.get('rac_scope', 'pat')
databricks_instance = 'adb-984752964297111.11.azuredatabricks.net'

# COMMAND ----------

endpoint = f'https://{databricks_instance}/api/2.0/sql/history/queries'

# COMMAND ----------

auth = {"Authorization": f"Bearer {pat}"}

# COMMAND ----------

response = requests.get(endpoint, headers=auth, json={'include_metrics': True, 'filter_by':{"warehouse_ids": ['f7732962603ec411']}})

# COMMAND ----------

data = json.loads(response.content.decode('utf-8'))
data

# COMMAND ----------

len(data.get('res'))

# COMMAND ----------

with open('/dbfs/tmp/json_queries2.json', 'w') as f:
  json.dump(data.get('res'), f)

# COMMAND ----------

df = (spark.read.json("/tmp/json_queries2.json"))
display(df)

# COMMAND ----------

1666359146295 - execution end time 
1666359146322 - query end time

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import * 
qids = [a.query_id for a in df.filter(col("executed_as_user_id") == "6563461962580955").select('query_id').distinct().collect()]

# COMMAND ----------


# for q in qids:
q = qids[0]
d = requests.delete(f"https://adb-984752964297111.11.azuredatabricks.net/api/2.0/preview/sql/queries/{q}", headers=auth)

# COMMAND ----------

d.content

# COMMAND ----------

q

# COMMAND ----------


