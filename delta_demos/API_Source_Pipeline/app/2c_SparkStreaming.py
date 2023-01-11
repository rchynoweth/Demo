# Databricks notebook source
# MAGIC %md
# MAGIC # Process JSON Data with Spark Structured Streaming 

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

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

# MAGIC %md
# MAGIC ## Read Delta Table as a Stream 

# COMMAND ----------

delta_df = (
  spark.readStream.format('delta').table('bronze_weather_api')
)

# COMMAND ----------

silver_df = (delta_df.select(delta_df.coord.lon.alias('lon')
                        , delta_df.coord.lat.alias('lat')
                        , delta_df.main.feels_like.alias("perceived_temp")
                        , delta_df.main.pressure.alias("pressure")
                        , delta_df.main.humidity.alias("humidity")
                        , delta_df.main.temp.alias("temp")
                        , delta_df.main.temp_min.alias("temp_min")
                        , delta_df.main.temp_max.alias("temp_max")
                       )
       )

# COMMAND ----------

# DBTITLE 1,Append Mode
append_ckpt = "/Users/{}/api_weather_demo/ckpt/append_ckpt".format(user_name)
dbutils.fs.rm(append_ckpt, True)
spark.sql("drop table if exists streaming_silver_weather_main")

(silver_df
 .writeStream
 .outputMode("append")
 .option("checkpointLocation", append_ckpt)
 .table("streaming_silver_weather_main")

)

# COMMAND ----------

display(silver_df)

# COMMAND ----------

# DBTITLE 1,Complete Mode 
# Replaces the entire table with every
complete_ckpt = "/Users/{}/api_weather_demo/ckpt/complete_ckpt".format(user_name)
dbutils.fs.rm(complete_ckpt, True)
spark.sql("drop table if exists streaming_silver_weather_main_agg")

(silver_df
 .groupBy("lon", "lat")
 .count()
 .writeStream
 .outputMode("complete")
 .option("checkpointLocation", complete_ckpt)
 .table("streaming_silver_weather_main_agg")

)

# COMMAND ----------

display(spark.sql("SELECT * FROM streaming_silver_weather_main_agg"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Json Files as a Stream Using AutoLoader

# COMMAND ----------

raw_data_directory = "/Users/{}/api_weather_demo/raw/*.json".format(user_name)
schema_directory = "/Users/{}/api_weather_demo/schemas/autoLoader_raw_to_silver".format(user_name)

# COMMAND ----------

autoLoader_df = (spark.readStream.format("cloudfiles")
                      .option("cloudFiles.format", "json")
                      .option("cloudFiles.schemaLocation", schema_directory)
                      .option("rescuedDataColumn", "_rescue")
                      .load(raw_data_directory)
                )

# COMMAND ----------

coord_schema = StructType([
  StructField("lon", DoubleType()),
  StructField("lat", DoubleType())
])

main_schema = StructType([
  StructField("temp", DoubleType()),
  StructField("feels_like", DoubleType()),
  StructField("temp_min", DoubleType()),
  StructField("temp_max", DoubleType()),
  StructField("pressure", DoubleType()),
  StructField("humidity", DoubleType()),
  StructField("sea_level", DoubleType()),
  StructField("grnd_level", DoubleType())
])


silver_al_df = (autoLoader_df
  .withColumn("coord_struct", from_json(col("coord"), coord_schema))
  .withColumn("main_struct", from_json(col("main"), main_schema))
)

# COMMAND ----------

display(silver_al_df)

# COMMAND ----------

## 
## For each batch function to use in the streaming 
## 
def transform_stream(microBatchDF, batchId):
  df = microBatchDF.select(microBatchDF.coord_struct.lon.alias('lon')
                        , microBatchDF.coord_struct.lat.alias('lat')
                        , microBatchDF.main_struct.feels_like.alias("perceived_temp")
                        , microBatchDF.main_struct.pressure.alias("pressure")
                        , microBatchDF.main_struct.humidity.alias("humidity")
                        , microBatchDF.main_struct.temp.alias("temp")
                        , microBatchDF.main_struct.temp_min.alias("temp_min")
                        , microBatchDF.main_struct.temp_max.alias("temp_max")
                        , microBatchDF.name.alias("name")
                  )
  
  (df.write
     .format("delta")
     .mode("append")
     .saveAsTable("autoLoader_silver_weather_main") 
  )


# COMMAND ----------

al_ckpt = "/Users/{}/api_weather_demo/ckpt/al_ckpt".format(user_name)
dbutils.fs.rm(al_ckpt, True)
spark.sql("drop table if exists autoLoader_silver_weather_main")


(silver_al_df.writeStream
    .format("delta")
    .option("checkpointLocation", al_ckpt)
    .trigger(once=True)
    .foreachBatch(transform_stream)
    .outputMode("update")
    .start()
)


# COMMAND ----------

import time

time.sleep(60)

# COMMAND ----------

display(
  spark.sql("SELECT * FROM autoLoader_silver_weather_main")
)

# COMMAND ----------

time.sleep(60)

for s in spark.streams.active:
  s.stop()
