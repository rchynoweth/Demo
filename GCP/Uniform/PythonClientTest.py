# Databricks notebook source
# MAGIC %md
# MAGIC Please note that I have a service account attached to the cluster. When this is the case the python client for BQ automatically authenticates as needed. 

# COMMAND ----------

# MAGIC %pip install google-cloud-bigquery

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from libs.bq_connect import BQConnect

# COMMAND ----------

bq_project = '<insert project name>'

# COMMAND ----------

bq_connect = BQConnect(project=bq_project, dbutils=dbutils)

# COMMAND ----------

sql_qry = """select * from new_york_citibike_trips.gas_prices """

# COMMAND ----------

bq_connect.execute_query(sql_qry)

# COMMAND ----------


