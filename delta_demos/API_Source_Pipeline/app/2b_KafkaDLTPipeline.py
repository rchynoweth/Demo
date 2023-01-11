# Databricks notebook source
connectionString = dbutils.secrets.get(scope= "oetrta", key = "oneenv-eventhubs")
ehConf ={"eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)}

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/nyctaxi/tripdata/yellow")

# COMMAND ----------

sc = spark.read.csv('/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2009-01.csv.gz', header=True).schema

# COMMAND ----------

schema_location = "/tmp/nyctaxi/schema"
dbutils.fs.rm(schema_location, True)

df = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", schema_location)
  .load("/databricks-datasets/nyctaxi/tripdata/yellow/*.csv.gz")
   )

display(df)

# COMMAND ----------

df = spark.readStream.format("csv").option('header', 'true').load("/databricks-datasets/nyctaxi/tripdata/yellow/*.csv.gz")
display(df)

# COMMAND ----------



# COMMAND ----------


