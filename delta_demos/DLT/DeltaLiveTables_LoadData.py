# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Live Tables - Detailed 

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "")
database_name = dbutils.widgets.get("DatabaseName")

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS nyctaxi_bronze")

# COMMAND ----------

data_files = [f.path for f in dbutils.fs.ls("/databricks-datasets/nyctaxi/tripdata/green")]
len(data_files) 

# COMMAND ----------

sc = StructType(
[
  StructField('VendorID', IntegerType()),
  StructField('pickup_datetime',StringType()),
  StructField('dropoff_datetime',StringType()),
  StructField('Store_and_fwd_flag',StringType()),
  StructField('RateCodeID',IntegerType()),
  StructField('Pickup_longitude',DoubleType()),
  StructField('Pickup_latitude',DoubleType()),
  StructField('Dropoff_longitude',DoubleType()),
  StructField('Dropoff_latitude',DoubleType()),
  StructField('Passenger_count',IntegerType()),
  StructField('Trip_distance',DoubleType()),
  StructField('Fare_amount',DoubleType()),
  StructField('Extra',DoubleType()),
  StructField('MTA_tax',DoubleType()),
  StructField('Tip_amount',DoubleType()),
  StructField('Tolls_amount',DoubleType()),
  StructField('Ehail_fee',StringType()),
  StructField('Total_amount',DoubleType()),
  StructField('Payment_type',IntegerType()),
  StructField('Trip_type' ,StringType())
]  )
  

# COMMAND ----------

display(
spark.read.format("csv").schema(sc).option("header", True).load(data_files[0])
)

# COMMAND ----------

(spark.read
 .format("csv")
 .schema(sc)
 .option("header", True)
 .load(data_files[0])
 .write
 .mode("overwrite")
 .format("delta")
 .saveAsTable("nyctaxi_bronze"))

# COMMAND ----------

def load_data():
  i = 1
  while i < len(data_files):
    (spark.read
     .format("csv")
     .schema(sc)
     .option("header", True)
     .load(data_files[i])
     .write
     .mode("append")
     .format("delta")
     .saveAsTable("nyctaxi_bronze"))
    
    i+=1 
    yield i

# COMMAND ----------

next(load_data())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Examine the pipeline

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM $DatabaseName.nyctaxi_daily_aggs

# COMMAND ----------


