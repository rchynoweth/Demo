# Databricks notebook source
# MAGIC %md
# MAGIC # Load Silver Data 
# MAGIC 
# MAGIC The purpose of this notebook is to demonstrate the triggers available in [Databricks Workflows](https://docs.databricks.com/workflows). 
# MAGIC 
# MAGIC - [Continuous Job](https://docs.databricks.com/workflows/jobs/jobs.html#run-a-continuous-job)  
# MAGIC - [File Arrival Job](https://docs.databricks.com/workflows/jobs/jobs.html#run-a-job-when-new-files-arrive)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# widget parameters 
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

input_table = "bronze_iot_demo"

# COMMAND ----------

spark.sql(f"create catalog if not exists {catalog}")
spark.sql(f"use catalog {catalog}")
spark.sql(f"create schema if not exists {schema}")
spark.sql(f"use schema {schema}")

# COMMAND ----------

df = spark.sql("select * from bronze_iot_demo")

# COMMAND ----------

(
    df
    .selectExpr("count(1) as cnt")
    .write
    .mode("overwrite")
    .saveAsTable("silver_iot_demo")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_iot_demo

# COMMAND ----------


