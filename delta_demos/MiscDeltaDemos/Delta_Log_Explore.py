# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *

# COMMAND ----------

df = (spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('/databricks-datasets/timeseries/Fires/Fire_Department_Calls_for_Service.csv'))
for c in df.columns:
  df = df.withColumnRenamed(c, c.replace(' ', '').replace('-', ''))
  
df.coalesce(1).write.mode("overwrite").format("parquet").save("/tmp/fires")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE Fires_Bronze
  (
  CallNumber int,
  UnitID string,
  IncidentNumber int,
  CallType string,
  CallDate string,
  WatchDate string,
  ReceivedDtTm string,
  EntryDtTm string,
  DispatchDtTm string,
  ResponseDtTm string,
  OnSceneDtTm string,
  TransportDtTm string,
  HospitalDtTm string,
  CallFinalDisposition string,
  AvailableDtTm string,
  Address string,
  City string,
  ZipcodeofIncident int,
  Battalion string,
  StationArea string,
  Box string,
  OriginalPriority string,
  Priority string,
  FinalPriority int,
  ALSUnit boolean,
  CallTypeGroup string,
  NumberofAlarms int,
  UnitType string,
  Unitsequenceincalldispatch int,
  FirePreventionDistrict string,
  SupervisorDistrict string,
  NeighborhooodsAnalysisBoundaries string,
  Location string,
  RowID string
  )
PARTITIONED BY (UnitID)

""")

# COMMAND ----------

spark.sql("""
COPY INTO Fires_Bronze
FROM '/tmp/fires'
FILEFORMAT = PARQUET
""")

# COMMAND ----------

display(spark.sql("SELECT * FROM fires_bronze"))

# COMMAND ----------

table_location = (
  spark.sql("DESCRIBE EXTENDED fires_bronze").filter(col("col_name") == "Location").filter(col("data_type") != "string")
    .select("data_type").collect()[0][0].replace("dbfs:", "")
)
print(table_location)

# COMMAND ----------

dbutils.fs.ls(table_location+"/_delta_log")

# COMMAND ----------

display(spark.read.json(table_location+"/_delta_log/*.json").withColumn("filename", input_file_name()).orderBy("filename"))

# COMMAND ----------

display(spark.read.json(table_location+"/_delta_log/*.crc").withColumn("filename", input_file_name()).orderBy("filename"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW merge_data 
# MAGIC AS (
# MAGIC (SELECT CallNumber ,
# MAGIC UnitID ,
# MAGIC IncidentNumber ,
# MAGIC CallType ,
# MAGIC CallDate ,
# MAGIC WatchDate ,
# MAGIC ReceivedDtTm ,
# MAGIC EntryDtTm ,
# MAGIC DispatchDtTm ,
# MAGIC ResponseDtTm ,
# MAGIC OnSceneDtTm ,
# MAGIC TransportDtTm ,
# MAGIC HospitalDtTm ,
# MAGIC 'THIS ROW HAS BEEN UPDATED' as CallFinalDisposition ,
# MAGIC AvailableDtTm ,
# MAGIC Address ,
# MAGIC City ,
# MAGIC ZipcodeofIncident ,
# MAGIC Battalion ,
# MAGIC StationArea ,
# MAGIC Box ,
# MAGIC OriginalPriority ,
# MAGIC Priority ,
# MAGIC FinalPriority ,
# MAGIC ALSUnit ,
# MAGIC CallTypeGroup ,
# MAGIC NumberofAlarms ,
# MAGIC UnitType ,
# MAGIC Unitsequenceincalldispatch ,
# MAGIC FirePreventionDistrict ,
# MAGIC SupervisorDistrict ,
# MAGIC NeighborhooodsAnalysisBoundaries ,
# MAGIC Location ,
# MAGIC RowID  
# MAGIC FROM fires_bronze
# MAGIC 
# MAGIC WHERE UnitID = 'T13'
# MAGIC 
# MAGIC LIMIT 499
# MAGIC )
# MAGIC UNION 
# MAGIC (
# MAGIC SELECT CallNumber ,
# MAGIC UnitID ,
# MAGIC IncidentNumber ,
# MAGIC CallType ,
# MAGIC CallDate ,
# MAGIC WatchDate ,
# MAGIC ReceivedDtTm ,
# MAGIC EntryDtTm ,
# MAGIC DispatchDtTm ,
# MAGIC ResponseDtTm ,
# MAGIC OnSceneDtTm ,
# MAGIC TransportDtTm ,
# MAGIC HospitalDtTm ,
# MAGIC 'THIS ROW HAS BEEN UPDATED' as CallFinalDisposition ,
# MAGIC AvailableDtTm ,
# MAGIC Address ,
# MAGIC City ,
# MAGIC ZipcodeofIncident ,
# MAGIC Battalion ,
# MAGIC StationArea ,
# MAGIC Box ,
# MAGIC OriginalPriority ,
# MAGIC Priority ,
# MAGIC FinalPriority ,
# MAGIC ALSUnit ,
# MAGIC CallTypeGroup ,
# MAGIC NumberofAlarms ,
# MAGIC UnitType ,
# MAGIC Unitsequenceincalldispatch ,
# MAGIC FirePreventionDistrict ,
# MAGIC SupervisorDistrict ,
# MAGIC NeighborhooodsAnalysisBoundaries ,
# MAGIC Location ,
# MAGIC RowID  
# MAGIC FROM fires_bronze
# MAGIC 
# MAGIC WHERE UnitID = 'AM62'
# MAGIC 
# MAGIC LIMIT 499
# MAGIC )
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM merge_data

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO fires_bronze as target  
# MAGIC USING merge_data as source ON source.RowID = target.RowID
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

dbutils.fs.ls(table_location+"/_delta_log")

# COMMAND ----------

display(spark.read.json("/user/hive/warehouse/fires_bronze/_delta_log/00000000000000000003.json"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct unitid from fires_bronze
