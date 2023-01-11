# Databricks notebook source
# MAGIC %md
# MAGIC # Using Databricks AutoLoader for Parquet files 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Environment and Demo Setup

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "rac_demo_db")
database_name = dbutils.widgets.get("DatabaseName")
dbutils.widgets.text("UserName", "ryan.chynoweth@databricks.com")
user_name = dbutils.widgets.get("UserName")

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
spark.sql("USE {}".format(database_name))

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from shutil import copyfile
from datetime import timedelta
import os 

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS Fires

# COMMAND ----------

dbutils.fs.rm("/Users/{}/bronze/autoloader_parquet_demo".format(user_name), True)

# COMMAND ----------

dbutils.fs.rm("/Users/{}/silver/autoloader_parquet_demo".format(user_name), True)

# COMMAND ----------

dbutils.fs.mkdirs("/Users/{}/bronze/autoloader_parquet_demo".format(user_name))

# COMMAND ----------

df = (spark
      .read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/databricks-datasets/timeseries/Fires/Fire_Department_Calls_for_Service.csv")

)

# https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
df = df.withColumn("Call Date", to_timestamp(col("Call Date"), "MM/dd/yyyy"))
df = df.withColumn("Watch Date", to_timestamp(col("Watch Date"), "MM/dd/yyyy"))
df = df.withColumn("Received DtTm", to_timestamp(col("Received DtTm"), 'MM/dd/yyyy hh:mm:ss a')) 
df = df.withColumn("Entry DtTm", to_timestamp(col("Entry DtTm"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("Dispatch DtTm", to_timestamp(col("Dispatch DtTm"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("Response DtTm", to_timestamp(col("Response DtTm"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("On Scene DtTm", to_timestamp(col("On Scene DtTm"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("Transport DtTm", to_timestamp(col("Transport DtTm"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("Hospital DtTm", to_timestamp(col("Hospital DtTm"), 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn("Available DtTm", to_timestamp(col("Available DtTm"), 'MM/dd/yyyy hh:mm:ss a'))

display(df)

# COMMAND ----------

for c in df.columns:
  df = df.withColumnRenamed(c, c.replace(" ", ""))

# COMMAND ----------

tables = [t.name for t in spark.catalog.listTables(database_name)]
if "fires" not in tables:
  df.write.format("delta").saveAsTable("Fires")

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize Fires ZORDER BY (CallDate) 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Fires limit 50

# COMMAND ----------

if os.path.exists("/dbfs/tmp/Fires"):
  dbutils.fs.rm("/tmp/Fires", True)

# COMMAND ----------

# DBTITLE 1,Function to save parquet files 
def publish_parquet_file():
  current_date = spark.sql("SELECT min(`CallDate`) from Fires").collect()[0][0]
  max_date = spark.sql("SELECT max(`CallDate`) from Fires").collect()[0][0]

  while current_date < max_date:
    # select 1 day of data and write it as single file
    temp_df = spark.sql("""
      SELECT * 
      FROM Fires
      WHERE `CallDate` = '{}'
      """.format(current_date))
    
    print("cdt: {} | rn: {}".format(current_date, temp_df.count()))
    temp_df.coalesce(1).write.format('parquet').mode("overwrite").save("/tmp/Fires/")
    
    # copy single parquet file
    dirs = os.listdir("/dbfs/tmp/Fires")
    parquet_file = dirs[-1]
    copyfile("/dbfs/tmp/Fires/{}".format(parquet_file), "/dbfs/Users/{}/bronze/autoloader_parquet_demo/{}".format(user_name, parquet_file))
    
    
    yield current_date
    current_date = current_date + timedelta(days=1)

# COMMAND ----------

dbutils.fs.ls("/Users/{}/bronze/autoloader_parquet_demo/".format(user_name))

# COMMAND ----------

file_creator = publish_parquet_file()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Publish files and set up first AutoLoader run 

# COMMAND ----------

dir_path = "/Users/{}/bronze/autoloader_parquet_demo/".format(user_name)
data_path = "/Users/{}/bronze/autoloader_parquet_demo/*.parquet".format(user_name)

# COMMAND ----------

# create some files
for i in range(0,3):
  print(next(file_creator))

# COMMAND ----------

latest_file = dbutils.fs.ls(dir_path)[-1].path
# latest_file
sc = spark.read.parquet(latest_file).schema
sc

# COMMAND ----------

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .schema(sc)
#     .option("cloudFiles.schemaLocation", bronzeCheckpoint)
    .load(data_path)
)

# COMMAND ----------

display(spark.sql("DESCRIBE SCHEMA {}".format(database_name)))

# COMMAND ----------

output_table_location = spark.sql("DESCRIBE SCHEMA {}".format(database_name)).filter(col("database_description_item") == "Location").select(col("database_description_value")).collect()[0][0] +"/Fires_AutoLoader_Parquet"
checkpoint_location = output_table_location + "/_checkpoint"

# COMMAND ----------

dbutils.fs.ls("dbfs:/user/hive/warehouse/rac_demo_db.db/address")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Handling 
# MAGIC 
# MAGIC AutoLoader does not support schema inference and schema evolution for parquet files. Therefore, in order to handle schema evolution for parquet you need to do something like the following: 
# MAGIC - Read the latest file and get the schema using `spark.read.parquet().schema` 
# MAGIC   - This will be provided when using reading data with autoloader 
# MAGIC   - Look above this block for a code sample 
# MAGIC - Read the data into a streaming DF 
# MAGIC - Compare the DF to your target table and handle any schema changes as needed 
# MAGIC 
# MAGIC Below I simply look for new columns that I need to add. But you can also handle data type changes and what not as well.  
# MAGIC 
# MAGIC 
# MAGIC Note this is something we are working on to fix so that this type of workflow is not required. 

# COMMAND ----------

# df.columns # column names
df.dtypes # column names and column types

# COMMAND ----------

# DBTITLE 1,Schema Evolution Logic!
# check for columns that need to be added 
target_cols = spark.read.table("Fires_AutoLoader_Parquet").columns

for c, d in df.dtypes:
  if c not in target_cols:
    spark.sql("ALTER TABLE Fires_AutoLoader_Parquet ADD COLUMN ( {} {} ".format(c, d))

# COMMAND ----------

df.writeStream.trigger(once=True).option("checkpointLocation", checkpoint_location).table("Fires_AutoLoader_Parquet")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM Fires_AutoLoader_Parquet

# COMMAND ----------

# create some files
for i in range(0,3):
  print(next(file_creator))

# COMMAND ----------

##### 1047
# cdt: 2000-04-12 00:00:00 | rn: 57
# 2000-04-12 00:00:00 
# cdt: 2000-04-13 00:00:00 | rn: 433
# 2000-04-13 00:00:00 
# cdt: 2000-04-14 00:00:00 | rn: 557
# 2000-04-14 00:00:00 


##### 1580
# cdt: 2000-04-15 00:00:00 | rn: 492
# 2000-04-15 00:00:00
# cdt: 2000-04-16 00:00:00 | rn: 536
# 2000-04-16 00:00:00
# cdt: 2000-04-17 00:00:00 | rn: 552

# COMMAND ----------

df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .schema(sc)
#     .option("cloudFiles.schemaLocation", bronzeCheckpoint)
    .load(data_path)
)

# COMMAND ----------

df.writeStream.trigger(once=True).option("checkpointLocation", checkpoint_location).table("Fires_AutoLoader_Parquet")

# COMMAND ----------

# create some files
for i in range(0,3):
  print(next(file_creator))

# COMMAND ----------

# def process_data(microBatchDF, batchId):
#   with open("/dbfs/{}/autoloader_demo/output/failure.json".format(mount_location), 'r') as f:
#     cnt = json.load(f).get('fail')

#   fail = True if cnt == 1 else False

#   if fail and DeltaTable.isDeltaTable(spark, delta_table):
#     microBatchDF = microBatchDF.withColumn("failCol", lit(1))
#     microBatchDF = microBatchDF.withColumn("file_name", input_file_name())
#     microBatchDF.write.format("delta").mode("append").save(delta_table)   
#   else :
#     microBatchDF = microBatchDF.withColumn("file_name", input_file_name())
#     microBatchDF.write.format("delta").mode("append").option("mergeSchema", "true").save(delta_table)
  
#   cnt+=1
#   data = {"fail": cnt}
#   with open("/dbfs/{}/autoloader_demo/output/failure.json".format(mount_location), 'w') as f:
#     print("----> Saving json file: {}".format(data))
#     json.dump(data, f)
    

# # COMMAND ----------

# (df.writeStream
#   .format("delta")
#   .option("checkpointLocation", checkpoint_path)
#   .option("cloudFiles.useNotifications", True)
#   .trigger(once=True)
#   .foreachBatch(process_data)
#   .outputMode("update")
#   .start()
# )

# COMMAND ----------

##### 1510
# cdt: 2000-04-18 00:00:00 | rn: 445
# 2000-04-18 00:00:00
# cdt: 2000-04-19 00:00:00 | rn: 493
# 2000-04-19 00:00:00
# cdt: 2000-04-20 00:00:00 | rn: 572
# 2000-04-20 00:00:00

# COMMAND ----------

def batch_func(microBatchDF, batchId): 
  microBatchDF.write.mode("append").saveAsTable("Fires_AutoLoader_Parquet")

# COMMAND ----------

df.writeStream.trigger(once=True).option("checkpointLocation", checkpoint_location).table("Fires_AutoLoader_Parquet")

(df.writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_location)
  .trigger(once=True)
  .foreachBatch(batch_func)
  .outputMode("append")
  .start()
)

# COMMAND ----------

spark.streams.active

# COMMAND ----------

import time 

while len(spark.streams.active):
  print("Streams are still open")
  time.sleep(30)
  

# COMMAND ----------

len(spark.streams.active)

# COMMAND ----------


