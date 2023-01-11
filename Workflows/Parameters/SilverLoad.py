# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

dbutils.jobs.taskValues.get(taskKey='Bronze', key = "bronze_max_datetime", debugValue='2022-01-01')

# COMMAND ----------

dbutils.widgets.text('schema', '')
dbutils.widgets.text('table', '')
dbutils.widgets.text('target_table_name', '')

# COMMAND ----------

schema_name = dbutils.widgets.get('schema')
table_name = dbutils.widgets.get('table')
target_table_name = dbutils.widgets.get('target_table_name')

# COMMAND ----------

spark.sql(f'create schema if not exists {schema_name}')

# COMMAND ----------

spark.sql(f'use {schema_name}')

# COMMAND ----------

df = spark.read.table(table_name)
display(df)

# COMMAND ----------

# group_df = df.groupBy(col('device_id'), window(col('timestamp'), "5 minutes")).sum("calories_burnt")
group_df = (df.groupBy(col('device_id'), window(col('timestamp'), '5 minutes'))
            .agg(sum('calories_burnt').alias('total_calories_burnt'),
                 sum('num_steps').alias('total_steps'),
                 sum('miles_walked').alias('total_miles_walked'),
                 count('*').alias('reading_count'),
                )
            .withColumn('windowStart', col('window').start)
            .withColumn('windowEnd', col('window').end)
            .drop(col('window'))
           )

# COMMAND ----------

display(group_df)

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS {}
(
device_id long,
total_calories_burnt double,
total_steps long,
total_miles_walked double,
reading_count long,
windowStart timestamp,
windowEnd timestamp
)
""".format(target_table_name))

# COMMAND ----------

delta_table = DeltaTable.forName(spark, target_table_name)

# COMMAND ----------

(delta_table.alias('target').merge(
  group_df.alias('source'), 
  "target.device_id = source.device_id and target.windowStart = source.windowStart"
  )
 .whenMatchedUpdateAll()
 .whenNotMatchedInsertAll()
).execute()

# COMMAND ----------

display(spark.read.table(target_table_name).orderBy(col('windowStart').desc()))

# COMMAND ----------


