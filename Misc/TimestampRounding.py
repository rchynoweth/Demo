# Databricks notebook source
# MAGIC %md
# MAGIC # Timestamp Rounding 
# MAGIC 
# MAGIC This notebook explores the methods of rounding timestamps to the nearest interval. In our case we will round timestamps as 30 minute floors (2:22 becomes 2:00).  

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# create a spark dataframe

dts = ['2021-11-05 01:27:00', '2021-11-05 01:20:00', '2021-11-05 05:45:33', '2021-11-05 12:38:12', '2021-11-05 17:59:59']
pdf = pd.DataFrame({'datetime_values': dts})
df = spark.createDataFrame(pdf)
display(df)

# COMMAND ----------

df = (df.withColumn("new_minute", when(minute(col("datetime_values")) >= 30, 30).otherwise(0))
  .withColumn("hour", hour(col("datetime_values")))
  .withColumn("date", col("datetime_values").cast(DateType()))
  .withColumn("new_datetime", from_unixtime(unix_timestamp(col("date").cast(TimestampType())) + (col("hour")*3600) + col("new_minute")*60))
  )
display(df)

# COMMAND ----------

display(df.drop('new_minute', 'hour', 'date'))

# COMMAND ----------

# MAGIC %sql
# MAGIC USE rac_demo_db

# COMMAND ----------

df.write.format("delta").saveAsTable("window_time_table")
