# Databricks notebook source
# Import functions
from pyspark.sql.functions import input_file_name, current_timestamp, col
from pyspark.sql.window import Window

# COMMAND ----------


# Define variables used in code below
file_path = "/databricks-datasets/structured-streaming/events"
username = spark.sql("SELECT regexp_replace(current_user(), '[^a-zA-Z0-9]', '_')").first()[0]
table_name = f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# COMMAND ----------

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Finding Duplicates in a Dataset

# COMMAND ----------

# read the data twice to create duplicates
df1 = spark.read.json(file_path).withColumn('processing_time', current_timestamp())
df2 = spark.read.json(file_path).withColumn('processing_time', current_timestamp())
df = df1.union(df2)

df_duplicates = df.groupBy(df.columns).count().where(col("count") > 1)

# COMMAND ----------

display(df_duplicates)

# COMMAND ----------

display(df.dropDuplicates())

# COMMAND ----------

# MAGIC %md
# MAGIC # Read JSON files with AutoLoader

# COMMAND ----------

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "json")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", input_file_name().alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))

# COMMAND ----------

display(spark.read.table(table_name))

# COMMAND ----------


