# Databricks notebook source


# COMMAND ----------

# MAGIC %sql
# MAGIC use rac_demo_db

# COMMAND ----------

df = spark.readStream.format("delta").table("sensor_readings_historical_bronze")

# COMMAND ----------

dbutils.fs.ls("/Users/ryan.chynoweth@databricks.com/silver/iot_delta_table_to_list_contents")

# COMMAND ----------

delta_path = "/Users/ryan.chynoweth@databricks.com/silver/test_streaming_table"
checkpoint_path = "/Users/ryan.chynoweth@databricks.com/silver/test_streaming_table/_checkpoint"

# COMMAND ----------

(df.writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_path)
  .trigger(once=True)
  .start(delta_path)
)

# COMMAND ----------


