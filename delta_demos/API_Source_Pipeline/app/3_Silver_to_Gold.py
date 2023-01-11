# Databricks notebook source
# MAGIC %md
# MAGIC # Processing Silver Data to Gold

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

dbutils.widgets.text("schema_name", "") ### Note - this can be a widget or an environment variable  
schema_name = dbutils.widgets.get("schema_name")

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}".format(schema_name))

# COMMAND ----------

spark.sql("USE {}".format(schema_name))

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE batch_gold_table
AS 
SELECT main.feels_like as perceived_temp
, main.humidity
, main.pressure
, main.temp
, main.temp_max
, main.temp_min
, name
, coord.lat
, coord.lon
, from_unixtime(dt) as datetime

FROM batch_sliver_table
""")

# COMMAND ----------

display(spark.sql("SELECT * FROM batch_gold_table"))

# COMMAND ----------


