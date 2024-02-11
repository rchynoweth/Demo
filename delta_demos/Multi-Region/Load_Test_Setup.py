# Databricks notebook source
# MAGIC %md
# MAGIC # Load Test for Cross Region Latency

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set source data on columbus

# COMMAND ----------

bucket_name = 'rac_columbus'
storage_location = f'gs://{bucket_name}'
dbutils.fs.ls(storage_location)

# COMMAND ----------

df = spark.read.option('header', 'true').csv('/databricks-datasets/nyctaxi/tripdata/yellow/*.csv.gz')
display(df)

# COMMAND ----------

df.write.save(f'gs://{bucket_name}/source/nyc_delta')

# COMMAND ----------

spark.sql(f"optimize delta.`gs://{bucket_name}/source/nyc_delta`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set source data on central

# COMMAND ----------

central_bucket_name = 'rac_central'
central_storage_location = f'gs://{central_bucket_name}'
dbutils.fs.ls(central_storage_location)

# COMMAND ----------

spark.sql(f"""CREATE TABLE delta.`gs://{central_bucket_name}/source/nyc_delta` CLONE delta.delta.`gs://{bucket_name}/source/nyc_delta` """)

# COMMAND ----------

spark.sql(f"optimize delta.`gs://{central_bucket_name}/source/nyc_delta`")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Size

# COMMAND ----------

files = dbutils.fs.ls(f'gs://{bucket_name}/source/nyc_delta')
size = 0
for f in files: 
  size+=f.size
print(f"Total GBs: {size/1000000000}")

# COMMAND ----------

files = dbutils.fs.ls(f'gs://{central_bucket_name}/source/nyc_delta')
size = 0
for f in files: 
  size+=f.size
print(f"Total GBs: {size/1000000000}")

# COMMAND ----------

files = dbutils.fs.ls('/databricks-datasets/nyctaxi/tripdata/yellow/')
size = 0
for f in files: 
  size+=f.size
print(f"Total GBs: {size/1000000000}")

# COMMAND ----------

display(spark.sql(f"describe history delta.`gs://{central_bucket_name}/source/nyc_delta`"))

# COMMAND ----------

display(spark.sql(f"describe history delta.`gs://{bucket_name}/source/nyc_delta`"))

# COMMAND ----------


