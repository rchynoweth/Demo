# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists rac_demo_db

# COMMAND ----------

# MAGIC %sql
# MAGIC use rac_demo_db

# COMMAND ----------

dbutils.fs.ls("/databricks-datasets/airlines/")

# COMMAND ----------

# DBTITLE 1,Airlines Dataset 
sc = StructType(
  [  
    StructField("Year", IntegerType()),
    StructField("Month", IntegerType()),
    StructField("DayofMonth", IntegerType()),
    StructField("DayOfWeek", IntegerType()),
    StructField("DepTime", StringType()),
    StructField("CRSDepTime", IntegerType()),
    StructField("ArrTime", StringType()),
    StructField("CRSArrTime", IntegerType()),
    StructField("UniqueCarrier", StringType()),
    StructField("FlightNum", IntegerType()),
    StructField("TailNum", StringType()),
    StructField("ActualElapsedTime", StringType()),
    StructField("CRSElapsedTime", IntegerType()),
    StructField("AirTime", StringType()),
    StructField("ArrDelay", StringType()),
    StructField("DepDelay", StringType()),
    StructField("Origin", StringType()),
    StructField("Dest", StringType()),
    StructField("Distance", StringType()),
    StructField("TaxiIn", StringType()),
    StructField("TaxiOut", StringType()),
    StructField("Cancelled", IntegerType()),
    StructField("CancellationCode", StringType()),
    StructField("Diverted", IntegerType()),
    StructField("CarrierDelay", StringType()),
    StructField("WeatherDelay", StringType()),
    StructField("NASDelay", StringType()),
    StructField("SecurityDelay", StringType()),
    StructField("LateAircraftDelay", StringType()),
    StructField("IsArrDelayed", StringType()),
    StructField("IsDepDelayed", StringType())
  ]
)



df = (
   spark
  .read
  .format("csv")
#   .option("inferSchema", True)
  .schema(sc)
  .option("header", False) # first file has headers but the rest do not
  .load("/databricks-datasets/airlines/part-*") 
)



# COMMAND ----------

(df.write
  .format("delta")
  .mode("overwrite")
  .saveAsTable("airlines_dataset"))

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize airlines_dataset

# COMMAND ----------

display(spark.sql("select * from airlines_dataset"))

# COMMAND ----------


