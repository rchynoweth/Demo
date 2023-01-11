-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Messing Around with Delta DDL Commands 
-- MAGIC 
-- MAGIC The purpose of this notebook is to highlight common functionality and assist with the onboarding of new Databricks users.  

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Set up  
-- MAGIC 1. Create [widgets](https://docs.databricks.com/notebooks/widgets.html) - Widgets are essentially notebook parameters.   
-- MAGIC 1. Set SQL variables  
-- MAGIC 1. Load data using Python 
-- MAGIC 1. Create my database
-- MAGIC 1. Set my default database

-- COMMAND ----------

-- DBTITLE 1,Create and set widgets in Python 
-- MAGIC %python
-- MAGIC dbutils.widgets.text("DatabaseName", "rac_demo_db")
-- MAGIC database_name = dbutils.widgets.get("DatabaseName")
-- MAGIC dbutils.widgets.text("UserName", "ryan.chynoweth@databricks.com")
-- MAGIC user_name = dbutils.widgets.get("UserName")
-- MAGIC dbutils.widgets.text("TablePath", "/users/{}/delta_tables/".format(user_name))
-- MAGIC table_path = dbutils.widgets.get("TablePath")

-- COMMAND ----------

-- SQL Version 
-- CREATE WIDGET TEXT y DEFAULT "10"

-- COMMAND ----------

SET var.file_path = "/tmp/fires";
SET var.file_type = PARQUET;
SET var.database_name = $DatabaseName; 
SET var.user_name = $UserName;
SET var.table_path = $TablePath

------ In SQL Server (TSQL) it was something like this 
-- DECLARE @MyVariable INT ;
-- SET @MyVariable = 0; 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col
-- MAGIC df = (spark.read.format('csv').option("header", "true").option("inferSchema", "true").load('/databricks-datasets/timeseries/Fires/Fire_Department_Calls_for_Service.csv'))
-- MAGIC for c in df.columns:
-- MAGIC   df = df.withColumnRenamed(c, c.replace(' ', '').replace('-', ''))
-- MAGIC   
-- MAGIC df.coalesce(1).write.mode("overwrite").format("parquet").save("/tmp/fires")

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS ${var.database_name}

-- COMMAND ----------

USE ${var.database_name};

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating and Working with Tables 

-- COMMAND ----------

DROP TABLE IF EXISTS ${var.database_name}.Fires_Bronze

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ${var.database_name}.Fires_Bronze
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

-- DBTITLE 1,Ingest Data into our Table 
COPY INTO $DatabaseName.Fires_Bronze
FROM ${var.file_path}
FILEFORMAT = ${var.file_type}

-- COMMAND ----------

DESCRIBE TABLE Fires_Bronze 

-- COMMAND ----------

DESCRIBE EXTENDED Fires_Bronze

-- COMMAND ----------

-- DBTITLE 1,Time Travel and Version History 
DESCRIBE HISTORY Fires_Bronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Using `spark.sql`
-- MAGIC 
-- MAGIC It is really common to use the `spark.sql` function from Python and Scala to execute SQL queries. It allows users to leverage other languages while keeping code simple by expressing commands in plain SQL.   

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## Multi line SQL uses three quotations
-- MAGIC df = spark.sql(""" 
-- MAGIC SELECT * 
-- MAGIC FROM Fires_Bronze
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## Multi line SQL uses one single(')/double(") quotations
-- MAGIC df = spark.sql("SELECT * FROM Fires_Bronze")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

-- COMMAND ----------

DESCRIBE DATABASE $DatabaseName

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Creating and Working with Views

-- COMMAND ----------

-- Normal View. Scoped to the Metastore.  
CREATE VIEW Fires_UnitID_E11 
AS 
SELECT * FROM Fires_Bronze WHERE UnitID = 'E11'

-- COMMAND ----------

SELECT * FROM Fires_UnitID_E11

-- COMMAND ----------

-- Temp Views. Scoped to the session and note the metastore 
CREATE OR REPLACE TEMPORARY VIEW Fires_UnitID_E11_Temp 
AS 
SELECT * FROM Fires_Bronze WHERE UnitID = 'E11'

-- COMMAND ----------

SELECT * FROM Fires_UnitID_E11_Temp

-- COMMAND ----------

SHOW VIEWS 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ## Common Table Expressions  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Other Notes 
-- MAGIC 
-- MAGIC - Cursors: we do not have cursors or loops in Spark SQL. Cursors were mostly a way for people to batch load and write data. This is not really required in Databricks. IF for whatever reason you do need a loop it is best to just use the functionality from Scala/Python/R.   
-- MAGIC - 
