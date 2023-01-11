# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Change Data Feed - A Practical Example
# MAGIC 
# MAGIC In this notebook I would like to demonstrate a practical example of using the [change data feed](https://docs.databricks.com/delta/delta-change-data-feed.html) capabilities of Databricks Delta to highlight the production best practices of using the feature.  
# MAGIC 
# MAGIC We will (not particularly in this order):
# MAGIC - Create a silver delta table   
# MAGIC - Manually maintain version checkpoints  
# MAGIC - Create a gold delta table  
# MAGIC - Update tables as needed  

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import os 

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "")
database_name = dbutils.widgets.get("DatabaseName")

spark.sql(f"USE {database_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- spark configuration for delta change data capture
# MAGIC -- This will auto set this for all tables created in this session
# MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataCapture = True

# COMMAND ----------

def load_data():
  """ Function to load data into delta """
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
     .saveAsTable("nyctaxi_silver_cdf"))
    
    i+=1 
    yield i

# COMMAND ----------

def cdf_checkpoint_read():
  """ Read the checkpoint file """
  if os.path.exists(cdf_checkpoint_location):
    # READ CHECKPOINT IF EXISTS 
    with open("/"+cdf_checkpoint_location.replace(":",""), 'r') as f:
      ckpt = f.readline(1)
    return ckpt
    
  else :
    # RETURN VERSION 0 if Checkoint does not exist 
    return 0
  
  
def cdf_checkpoint_update(version):
  """ Update the checkpoint file """
  # WRITE CHECKPOINT 
  with open("/"+cdf_checkpoint_location.replace(":",""), 'w') as f:
    f.write(version)
    

def cdf_get_version(table):
  """ Obtain the version number from a table """
  tables = [t.name for t in spark.catalog.listTables("rac_demo_db")]
  
  if table.lower() in tables:
    return str(spark.sql("DESCRIBE HISTORY {}".format(table)).select("version").collect()[0][0])
  else :
    return "0"
  
  

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

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS nyctaxi_silver_cdf;
# MAGIC DROP TABLE IF EXISTS nyctaxi_daily_gold_cdf;

# COMMAND ----------

data_files = [f.path for f in dbutils.fs.ls("/databricks-datasets/nyctaxi/tripdata/green")]

df = spark.read.format("csv").schema(sc).option("header", True).load(data_files[0])
df.write.format("delta").saveAsTable("nyctaxi_silver_cdf")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nyctaxi_silver_cdf

# COMMAND ----------

# MAGIC %md
# MAGIC ## Checkpoints 
# MAGIC 
# MAGIC Spark will ignore directories or files that begin with an underscore ("_"). As a best practice checkpoints are maintained on the sink location of the data processing. 
# MAGIC 
# MAGIC For example, if I have a silver table and a gold table and I am using Change Data Feed to process 

# COMMAND ----------

# DBTITLE 1,Setting Locations! - NOTE these would be parameterized at the notebook level!
database_location = spark.sql("describe database {}".format(database_name)).filter(col("database_description_item") == "Location").select(col("database_description_value")).collect()[0][0]
print("Database Location: '{}'".format(database_location))
silver_table_location = "{}/nyctaxi_silver_cdf".format(database_location)
gold_table_location = "{}/nyctaxi_daily_gold_cdf".format(database_location)
cdf_checkpoint_location = "{}/nyctaxi_daily_gold_cdf/_cdf_checkpoint.txt".format(database_location)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY nyctaxi_silver_cdf

# COMMAND ----------

# DBTITLE 1,Create version 0 of our gold table 
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE nyctaxi_daily_gold_cdf
# MAGIC USING DELTA 
# MAGIC AS 
# MAGIC WITH CTE AS 
# MAGIC (SELECT date(pickup_datetime) as pickup_date
# MAGIC , count(1) as pickup_counts
# MAGIC , lag(count(1)) OVER (ORDER BY date(pickup_datetime)) as yesterday_pickup_counts
# MAGIC 
# MAGIC from nyctaxi_silver_cdf
# MAGIC group by date(pickup_datetime)
# MAGIC )
# MAGIC 
# MAGIC SELECT pickup_date, pickup_counts
# MAGIC , yesterday_pickup_counts 
# MAGIC ,(pickup_counts - yesterday_pickup_counts) as daily_diff
# MAGIC 
# MAGIC from CTE

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM nyctaxi_daily_gold_cdf

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY nyctaxi_silver_cdf

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY nyctaxi_daily_gold_cdf

# COMMAND ----------

# DBTITLE 1,Update checkpoint
cdf_get_version("nyctaxi_silver_cdf")

# COMMAND ----------

cdf_checkpoint_update(cdf_get_version("nyctaxi_silver_cdf"))

# COMMAND ----------

# DBTITLE 1,Load some new data
next(load_data())
next(load_data())
next(load_data())

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY nyctaxi_silver_cdf

# COMMAND ----------

spark.sql("DESCRIBE HISTORY {}".format("nyctaxi_daily_gold_cdf")).select("version").collect()[0][0]

# COMMAND ----------

tables = [t.name for t in spark.catalog.listTables(database_name)]
exists = False if 'nyctaxi_daily_gold_cdf' not in tables else True
ckpt = cdf_checkpoint_read()
ckpt

# COMMAND ----------

spark.sql(""" 
SELECT *, 
  dense_rank() OVER (PARTITION BY VendorID, pickup_datetime ORDER BY _commit_version DESC) as rank
  FROM table_changes('nyctaxi_silver_cdf', {}) 
  WHERE _change_type != 'update_preimage'
""".format(ckpt+1)).createOrReplaceTempView('rank_cte')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM rank_cte

# COMMAND ----------

spark.sql(""" 
SELECT 
  VendorID,
  pickup_datetime,
  dropoff_datetime,
  Store_and_fwd_flag,
  RateCodeID,
  Pickup_longitude,
  Pickup_latitude,
  Dropoff_longitude,
  Dropoff_latitude,
  Passenger_count,
  Trip_distance,
  Fare_amount,
  Extra,
  MTA_tax,
  Tip_amount,
  Tolls_amount,
  Ehail_fee,
  Total_amount,
  Payment_type,
  Trip_type,
  _change_type
FROM rank_cte
  WHERE rank = 1 

""").createOrReplaceTempView("transaction_cte")

# COMMAND ----------

if not exists:
  spark.sql(""" 
    SELECT date(pickup_datetime) as pickup_date
    , count(1) as pickup_counts
    , lag(count(1)) OVER (ORDER BY date(pickup_datetime)) as yesterday_pickup_counts

    from transaction_cte
    group by date(pickup_datetime)

  """).createOrReplaceTempView("lag_cte")
  
else : 
  date_data = spark.sql("SELECT MIN(pickup_datetime), date_add(MIN(pickup_datetime),-1) FROM transaction_cte").collect()
  min_date = date_data[0][0]
  yesterday_date = date_data[0][1]
  # get an append the previous day to avoid nulls
  spark.sql(""" 
    SELECT date(pickup_datetime) as pickup_date
      , count(1) as pickup_counts
      , lag(count(1)) OVER (ORDER BY date(pickup_datetime)) as yesterday_pickup_counts

    from transaction_cte
    group by date(pickup_datetime)
    
    UNION 
    
    SELECT date(pickup_datetime) as pickup_date
      , count(1) as pickup_counts
      , lag(count(1)) OVER (ORDER BY date(pickup_datetime)) as yesterday_pickup_counts

    from nyctaxi_silver_cdf
    where pickup_datetime >= '{}' and pickup_datetime < '{}'
    group by date(pickup_datetime)

  """.format(min_date, yesterday_date)).createOrReplaceTempView("lag_cte")
  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM nyctaxi_silver_cdf where pickup_datetime >= '2013-08-31' and pickup_datetime < '2013-09-01 00:02:00'

# COMMAND ----------

df = spark.sql(""" 
SELECT pickup_date, pickup_counts
, yesterday_pickup_counts 
,(pickup_counts - yesterday_pickup_counts) as daily_diff

from lag_cte
""")
display(df)

# COMMAND ----------


if not exists :
  df.write.format('delta').saveAsTable('nyctaxi_daily_gold_cdf')
  
else :
  # MERGE data 
  # must get some more data in order to calculate diff
  

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO delta_cdc_sink
# MAGIC 
# MAGIC USING encounters_updates AS source
# MAGIC   ON delta_cdc_sink.PK = source.PK
# MAGIC   
# MAGIC WHEN MATCHED AND source._change_type = 'delete' THEN DELETE
# MAGIC 
# MAGIC WHEN MATCHED AND source._change_type = 'update_postimage' THEN 
# MAGIC   UPDATE SET START=source.start, STOP=source.stop, PATIENT=source.patient, ENCOUNTER=source.encounter, CODE=source.code, DESCRIPTION=source.description, PK=source.pk
# MAGIC   
# MAGIC WHEN NOT MATCHED AND source._change_type = 'insert' THEN 
# MAGIC   INSERT (START, STOP, PATIENT, ENCOUNTER, CODE, DESCRIPTION, PK) 
# MAGIC   VALUES (source.start, source.stop, source.patient, source.encounter, source.code, source.description, source.pk)
