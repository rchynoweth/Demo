# Databricks notebook source
dbutils.widgets.text('schema', '')
dbutils.widgets.text('table', '')

# COMMAND ----------

schema_name = dbutils.widgets.get('schema')
table_name = dbutils.widgets.get('table')

# COMMAND ----------

spark.sql(f'create schema if not exists {schema_name}')

# COMMAND ----------

spark.sql(f'use {schema_name}')

# COMMAND ----------

df = (spark.read.format("json").load("/databricks-datasets/iot-stream/data-device/*.json.gz"))
display(df)

# COMMAND ----------

 df.write.mode('overwrite').saveAsTable(table_name)

# COMMAND ----------

display(spark.sql(f'select * from {table_name} limit 10'))

# COMMAND ----------

max_datetime = spark.sql(f'select max(timestamp) from {table_name}').collect()[0][0]
print(max_datetime)

# COMMAND ----------

dbutils.jobs.taskValues.set(key = "bronze_max_datetime", value = max_datetime)

# COMMAND ----------


