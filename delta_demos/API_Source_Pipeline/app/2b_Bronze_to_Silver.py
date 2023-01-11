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

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS {}".format(schema_name))

# COMMAND ----------

spark.sql("USE {}".format(schema_name))

# COMMAND ----------

spark.sql("""
  COPY INTO delta.`/Users/{}/api_weather_demo/silver/batch_sliver_table`
  FROM '/Users/{}/api_weather_demo/raw/*.json'
  FILEFORMAT = JSON
""".format(user_name, user_name))

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS batch_sliver_table
AS 
SELECT * 
FROM delta.`/Users/{}/api_weather_demo/silver/batch_sliver_table`
""".format(user_name))

# COMMAND ----------

display(spark.sql("SELECT * FROM batch_sliver_table"))

# COMMAND ----------


