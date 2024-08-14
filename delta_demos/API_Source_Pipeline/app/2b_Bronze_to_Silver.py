# Databricks notebook source
# MAGIC %md
# MAGIC # Processing Raw (bronze) Data to Silver 

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

user_name = spark.sql("SELECT current_user()").collect()[0][0]
print(user_name)

# COMMAND ----------

dbutils.widgets.text("schema_name", "") ### Note - this can be a widget or an environment variable  
schema_name = dbutils.widgets.get("schema_name")
dbutils.widgets.text("catalog_name", "") ### Note - this can be a widget or an environment variable  
catalog_name = dbutils.widgets.get("catalog_name")

# COMMAND ----------

# spark.sql("CREATE CATALOG IF NOT EXISTS {}".format(catalog_name))
spark.sql(f"use catalog {catalog_name}")
spark.sql("CREATE SCHEMA IF NOT EXISTS {}".format(schema_name))


# COMMAND ----------

spark.sql("USE {}".format(schema_name))

# COMMAND ----------

spark.sql("set spark.databricks.delta.schema.autoMerge.enabled=true")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * FROM bronze_weather_api

# COMMAND ----------

spark.sql("""
  create or replace table batch_sliver_table  as select * FROM bronze_weather_api
""")

# COMMAND ----------

display(spark.sql("SELECT * FROM batch_sliver_table"))

# COMMAND ----------


