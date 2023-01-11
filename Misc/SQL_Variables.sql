-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Using Variables in Spark SQL
-- MAGIC 
-- MAGIC In [Databricks SQL]() users have the ability to [parameterize queries](https://docs.databricks.com/sql/user/queries/query-parameters.html) and provide those values at execution. With this functionality we can also repurpose to using this within our Databricks Notebooks as well. 
-- MAGIC 
-- MAGIC In this notebook we will demonstrate how to use both widgets and variables when writing Spark SQL. We will ingest data from DBFS and load it into a Delta table. 

-- COMMAND ----------

CREATE WIDGET TEXT DatabaseName DEFAULT '';
CREATE WIDGET TEXT UserName DEFAULT '';

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS $DatabaseName;
USE $DatabaseName;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC df = (spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('/databricks-datasets/timeseries/Fires/Fire_Department_Calls_for_Service.csv'))
-- MAGIC for c in df.columns:
-- MAGIC   df = df.withColumnRenamed(c, c.replace(' ', '').replace('-', ''))
-- MAGIC   
-- MAGIC df.coalesce(1).write.mode("overwrite").format("parquet").save("/tmp/fires")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Below we are creating a couple of variables, but please note that the `var.` prefix can be any value that you would like. Variables must be prefixed by `<text>.` in order to use them.  

-- COMMAND ----------

SET var.file_path = '/tmp/fires';
SET var.file_type = PARQUET;
SET var.database_name = $DatabaseName

-- COMMAND ----------

-- you can use a variable instead of a widget for selecting the database
use ${var.database_name}

-- COMMAND ----------

drop table if exists $DatabaseName.Fires_Bronze

-- COMMAND ----------

CREATE TABLE ${var.database_name}.Fires_Bronze
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
USING DELTA

-- COMMAND ----------

COPY INTO $DatabaseName.Fires_Bronze
FROM ${var.file_path}
FILEFORMAT = ${var.file_type}

-- COMMAND ----------

SELECT * FROM ${var.database_name}.Fires_Bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC Often in SQL Stored procedures users will have something like the following to dynamically look up and store results into a SQL variable. 
-- MAGIC ```sql
-- MAGIC DECLARE @sql_var as int
-- MAGIC SET @sql_var = select top 1 col1 from table_name
-- MAGIC ```
-- MAGIC 
-- MAGIC As you can see in the following command, in Spark SQL it will result in the query itself being that variable. But the query can still be used within SQL commands as shown above from a tradition T-SQL environment. This functionality works exactly how a user would expect.    

-- COMMAND ----------

SET var.callNumber = (select CallNumber from Fires_Bronze limit 1)


-- COMMAND ----------

${var.callNumber}

-- COMMAND ----------

select * from Fires_Bronze where CallNumber in (${var.callNumber}) -- NOTE: this may return a different call number than the previous cell

-- COMMAND ----------


