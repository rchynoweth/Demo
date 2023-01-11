-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Change Data Capture in Databricks Delta
-- MAGIC 
-- MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/delta-lake-logo-whitebackground.png" width=200/>
-- MAGIC 
-- MAGIC Types of Change Data Capture:
-- MAGIC 1. Date Modified - must track when rows are created and updated
-- MAGIC 1. Diff - requires the most computation
-- MAGIC 1. Triggers - requires a shadow table or external tools to track when rows are updated
-- MAGIC 1. Log-based CDC - additional storage required to track these changes (preferred method in Delta Lake)
-- MAGIC   - Delta CDC 
-- MAGIC   - Streaming Checkpoints i.e. Trigger Once
-- MAGIC 
-- MAGIC Delta CDC is log based!
-- MAGIC 
-- MAGIC We will show options 1, 2, and 4 in this notebook. 

-- COMMAND ----------

USE SB_OMOP600 -- select the database to use should be the same as the notebooks 1-3 for data setup

-- COMMAND ----------

-- drop tables if they exist
drop table if exists encounters_cdc_source;
drop table if exists date_modified_cdc_sink;
drop table if exists diff_cdc_sink;
drop table if exists delta_cdc_sink;
drop table if exists delta_streaming_sink;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # remove streaming checkpoint
-- MAGIC dbutils.fs.rm("/tmp/demo_checkpoints/streaming_checkpoint", True)

-- COMMAND ----------

-- spark configuration for delta change data capture
-- This will auto set this for all tables created in this session
set spark.databricks.delta.properties.defaults.enableChangeDataCapture = True

-- COMMAND ----------

-- DBTITLE 1,Read bronze CSV datasets and ingest into silver Delta tables
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import * 
-- MAGIC from pyspark.sql.types import *
-- MAGIC ehr_path = '/databricks-datasets/rwe/ehr/csv'
-- MAGIC # display(dbutils.fs.ls(ehr_path)) ## display list of files
-- MAGIC 
-- MAGIC encounters_data = "/databricks-datasets/rwe/ehr/csv/conditions.csv"
-- MAGIC 
-- MAGIC df = spark.read.format("csv").option("inferSchema", True).option("header", True).load(encounters_data)
-- MAGIC df = df.withColumn("PK", monotonically_increasing_id())
-- MAGIC df = df.withColumn("START", col("START").cast(TimestampType()))
-- MAGIC df = df.withColumn("STOP", col("STOP").cast(TimestampType()))
-- MAGIC df = df.withColumn("MODIFIED_DATE", current_timestamp())
-- MAGIC 
-- MAGIC 
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Create a delta table to use for cdc
-- MAGIC df.write.format("delta").saveAsTable("encounters_cdc_source")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Delta - Change Data Feed

-- COMMAND ----------

-- DBTITLE 1,Let's take a look at our dataset and view the history
select * from encounters_cdc_source

-- COMMAND ----------

describe history encounters_cdc_source

-- COMMAND ----------

-- DBTITLE 1,We execute updates and inserts and take a look at what that does to table history
UPDATE encounters_cdc_source set STOP = current_timestamp() where PATIENT = 'a45a0f84-1d16-4517-9aa8-271c59f339fd'

-- COMMAND ----------

describe history encounters_cdc_source

-- COMMAND ----------

SELECT * FROM table_changes('encounters_cdc_source', 1) -- get all the changes made in the last operation


-- COMMAND ----------

UPDATE encounters_cdc_source set STOP = current_timestamp() where PATIENT = 'd2f33060-6988-4e03-a481-439d10aabc83'

-- COMMAND ----------

UPDATE encounters_cdc_source set STOP = current_timestamp() where PATIENT = 'a45a0f84-1d16-4517-9aa8-271c59f339fd' -- this is the same patient as the first update 

-- COMMAND ----------

describe history encounters_cdc_source

-- COMMAND ----------

-- DBTITLE 1,Now let's take a look at the table versions between version 1 and 2
-- 
-- The output of this operation are the changes since version 1 of the delta table
-- NOTICE: 26 rows!? This is because we provide the pre-commit version of the row and the post-commit version of the row
--
SELECT * FROM table_changes('encounters_cdc_source', 1, 2)


-- COMMAND ----------

-- MAGIC %python
-- MAGIC max_id = spark.sql("select max(pk) from encounters_cdc_source").collect()[0][0]
-- MAGIC spark.sql("INSERT INTO encounters_cdc_source values (current_timestamp(), current_timestamp(), 'inserted_row', 'inserted_row', 123456, 'description', {},current_timestamp())".format(max_id))

-- COMMAND ----------

-- table changes since version 4
SELECT * FROM table_changes('encounters_cdc_source', 4)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Why does this matter? 
-- MAGIC 
-- MAGIC Let's say that you are processing files from a bronze/raw directory (CSV files like above), into a silver delta table. Next you would like to only process the changes from silver to gold. 
-- MAGIC 
-- MAGIC In this scenario, our silver table is `encounters_cdc_source` and our gold table is `delta_cdc_sink`. 

-- COMMAND ----------

-- DBTITLE 1,Create a gold table from the unaltered version of our silver table
-- duplicate the silver table from version 0
CREATE TABLE delta_cdc_sink
using delta 
as select * from encounters_cdc_source version as of 0

-- COMMAND ----------

select * from delta_cdc_sink

-- COMMAND ----------

-- DBTITLE 1,Get only the most recent changed records from our silver table
-- Now I want to select the changes between version 0 and version 1 and update my gold table
-- first let's create a temp view

CREATE OR REPLACE TEMPORARY VIEW encounters_updates
    AS 
    SELECT 
      START
      , STOP
      , PATIENT
      , ENCOUNTER
      , CODE
      , DESCRIPTION
      , PK
      , MODIFIED_DATE

      FROM (
        SELECT *, 

        dense_rank() OVER (PARTITION BY PK ORDER BY _commit_version DESC) as rank


        FROM table_changes('encounters_cdc_source', 1) 

        WHERE _change_type in ('update_postimage', 'insert')
      ) 
      WHERE rank = 1


-- COMMAND ----------

-- these are our cdc changes
select * from encounters_updates

-- COMMAND ----------

-- DBTITLE 1,Merge the changes into our gold table

MERGE INTO delta_cdc_sink
USING encounters_updates
ON delta_cdc_sink.PK = encounters_updates.PK
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *

-- COMMAND ----------

SELECT * FROM delta_cdc_sink

-- COMMAND ----------

-- MAGIC %md
-- MAGIC What just happened? 
-- MAGIC - Created a silver Databricks Delta table
-- MAGIC - Executed three update statements and an insert
-- MAGIC - Created a gold Databricks Delta table that was the exact same as the unaltered version of our silver table
-- MAGIC - Selected the changes from the four commands and merged them into our gold Delta table 
-- MAGIC 
-- MAGIC Key Point: Efficient log-based CDC processing is natively available in Databricks Delta!  
-- MAGIC Note: We only considered update and insert commands. If deletion is required then that will need to be completed using a second merge command to match the rows that should be deleted. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Streaming APIs with Checkpoints and Trigger Once (non-SQL)

-- COMMAND ----------

-- rollback changes using time travel

RESTORE TABLE encounters_cdc_source TO VERSION AS OF 0


-- COMMAND ----------

-- DBTITLE 1,Read data as a stream
-- MAGIC %scala 
-- MAGIC import org.apache.spark.sql.streaming.Trigger
-- MAGIC import io.delta.tables._
-- MAGIC 
-- MAGIC var df = spark.readStream.format("delta").option("ignoreChanges", "true").table("encounters_cdc_source")

-- COMMAND ----------

-- MAGIC %scala 
-- MAGIC def mergeBatchData(microBatchDF: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row], batchId: scala.Long) = {
-- MAGIC   
-- MAGIC   if (DeltaTable.isDeltaTable("delta_streaming_sink")){
-- MAGIC     var deltaTable = DeltaTable.forName(spark, "delta_streaming_sink") // set the delta table for upsert
-- MAGIC 
-- MAGIC     (deltaTable.alias("delta_table")
-- MAGIC     .merge(microBatchDF.alias("updates"), "updates.word = delta_table.word") // join dataframe 'updates' with delta table 'delta_table' on the key
-- MAGIC     .whenMatched().updateAll() // if we match a key then we update all columns
-- MAGIC     .whenNotMatched().insertAll() // if we do not match a key then we insert all columns
-- MAGIC     .execute() )
-- MAGIC   } else {
-- MAGIC     microBatchDF.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable("delta_streaming_sink")
-- MAGIC   }
-- MAGIC   
-- MAGIC   
-- MAGIC }

-- COMMAND ----------

-- DBTITLE 1,Write the initial/unchanged version of the silver table to gold
-- MAGIC %scala
-- MAGIC df.writeStream
-- MAGIC     .format("delta")
-- MAGIC     .option("checkpointLocation", "/tmp/demo_checkpoints/streaming_checkpoint")
-- MAGIC     .trigger(Trigger.Once())
-- MAGIC     .foreachBatch(mergeBatchData _)
-- MAGIC     .outputMode("update")
-- MAGIC     .start()

-- COMMAND ----------

-- DBTITLE 1,Make some changes to the silver table
-- make updates
UPDATE encounters_cdc_source set STOP = current_timestamp(), MODIFIED_DATE = current_timestamp() where PATIENT = 'a45a0f84-1d16-4517-9aa8-271c59f339fd' ;
UPDATE encounters_cdc_source set STOP = current_timestamp(), MODIFIED_DATE = current_timestamp() where PATIENT = 'd2f33060-6988-4e03-a481-439d10aabc83';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC max_id = spark.sql("select max(pk) from encounters_cdc_source").collect()[0][0]
-- MAGIC spark.sql("INSERT INTO encounters_cdc_source values (current_timestamp(), current_timestamp(), 'inserted_row', 'inserted_row', 123456, 'description', {},current_timestamp())".format(max_id))

-- COMMAND ----------

-- DBTITLE 1,Process the changes 
-- MAGIC %scala
-- MAGIC df.writeStream
-- MAGIC     .format("delta")
-- MAGIC     .option("checkpointLocation", "/tmp/demo_checkpoints/streaming_checkpoint")
-- MAGIC     .trigger(Trigger.Once())
-- MAGIC     .foreachBatch(mergeBatchData _)
-- MAGIC     .outputMode("update")
-- MAGIC     .start()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC What just happened? 
-- MAGIC 
-- MAGIC We used the Spark Streaming APIs to process only the changes between each micro-batch of data. By applying the `.trigger(Trigger.Once())` argument to the write we are able to shut down the stream after a single execution. In turn this allows us to batch process only the changes. 
-- MAGIC 
-- MAGIC Please note this functionality is not available in SQL. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Data Modified CDC
-- MAGIC 
-- MAGIC Only works if date modified field exists and users must maintain checkpoints. This is very common in a number of systems, especially ones that have trigger updates when rows are altered. 

-- COMMAND ----------

-- rollback changes using time travel

RESTORE TABLE encounters_cdc_source TO VERSION AS OF 0


-- COMMAND ----------

-- DBTITLE 1,Optimize and Z-Order by date for faster processing
-- best to zorder by date for faster queries

OPTIMIZE encounters_cdc_source
ZORDER BY (modified_date)

-- COMMAND ----------

-- DBTITLE 1,Create an unaltered gold table from silver
-- create gold table
CREATE TABLE date_modified_cdc_sink
using delta 
as select * from encounters_cdc_source 

-- COMMAND ----------

-- DBTITLE 1,Get the max modified date from the unaltered version of the table
-- MAGIC %python 
-- MAGIC # maintain checkpoint datetime
-- MAGIC checkpoint_date = str(spark.sql("SELECT max(modified_date) from encounters_cdc_source version as of 0 ").collect()[0][0])
-- MAGIC checkpoint_date

-- COMMAND ----------

-- DBTITLE 1,Make some updates
-- insert data and make updates
UPDATE encounters_cdc_source set STOP = current_timestamp(), MODIFIED_DATE = current_timestamp() where PATIENT = 'a45a0f84-1d16-4517-9aa8-271c59f339fd' ;
UPDATE encounters_cdc_source set STOP = current_timestamp(), MODIFIED_DATE = current_timestamp() where PATIENT = 'd2f33060-6988-4e03-a481-439d10aabc83';

-- COMMAND ----------

-- MAGIC %python
-- MAGIC max_id = spark.sql("select max(pk) from encounters_cdc_source").collect()[0][0]
-- MAGIC spark.sql("INSERT INTO encounters_cdc_source values (current_timestamp(), current_timestamp(), 'inserted_row', 'inserted_row', 123456, 'description', {},current_timestamp())".format(max_id))

-- COMMAND ----------

-- DBTITLE 1,Get the changes since the last time the table was queried using the python variable. 
-- MAGIC %python
-- MAGIC ## 
-- MAGIC ## NOTE: while this is written in Python, it is clear that this is still SQL syntax. The best way to have loops and variables in your SQL code is to use the 'spark.sql' method. 
-- MAGIC ## 
-- MAGIC spark.sql("""CREATE OR REPLACE TEMPORARY VIEW encounters_updates
-- MAGIC     AS 
-- MAGIC     SELECT 
-- MAGIC       START
-- MAGIC       , STOP
-- MAGIC       , PATIENT
-- MAGIC       , ENCOUNTER
-- MAGIC       , CODE
-- MAGIC       , DESCRIPTION
-- MAGIC       , PK
-- MAGIC       , MODIFIED_DATE
-- MAGIC 
-- MAGIC       FROM encounters_cdc_source
-- MAGIC       WHERE MODIFIED_DATE > '{}'""".format(checkpoint_date)
-- MAGIC          )
-- MAGIC 
-- MAGIC display(spark.sql("SELECT * FROM encounters_updates"))

-- COMMAND ----------

MERGE INTO date_modified_cdc_sink
USING encounters_updates
ON date_modified_cdc_sink.PK = encounters_updates.PK
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *

-- COMMAND ----------

-- MAGIC %md
-- MAGIC What just happened?
-- MAGIC 
-- MAGIC 1. We created a gold delta table from our silver table
-- MAGIC 1. We optimized and z-ordered the silver table by modified date which allows us to effeciently query the table by that field
-- MAGIC 1. We saved the max modified date to a python variable
-- MAGIC 1. Next we made some changes which updated existing rows' modified date
-- MAGIC 1. Finally, we selected all rows that have been changed since the datetime in step 3, and merged those records into our gold table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Data Diff 
-- MAGIC 
-- MAGIC This process used to be the last resort for doing CDC within a data lake system using Spark. However, this method should seldom, if ever, be used now that delta change data feed exists! 

-- COMMAND ----------

-- rollback changes using time travel

RESTORE TABLE encounters_cdc_source TO VERSION AS OF 0;

-- COMMAND ----------

-- DBTITLE 1,Create gold table
CREATE TABLE diff_cdc_sink 
AS 
SELECT * FROM encounters_cdc_source version as of 0

-- COMMAND ----------

-- DBTITLE 1,Make some changes
-- MAGIC %python
-- MAGIC # insert data
-- MAGIC max_id = spark.sql("select max(pk) from encounters_cdc_source").collect()[0][0]
-- MAGIC spark.sql("INSERT INTO encounters_cdc_source values (current_timestamp(), current_timestamp(), 'inserted_row', 'inserted_row', 123456, 'description', {},current_timestamp())".format(max_id))

-- COMMAND ----------

-- update some rows
UPDATE encounters_cdc_source set STOP = current_timestamp(), MODIFIED_DATE = current_timestamp() where PATIENT = 'a45a0f84-1d16-4517-9aa8-271c59f339fd' ;
UPDATE encounters_cdc_source set STOP = current_timestamp(), MODIFIED_DATE = current_timestamp() where PATIENT = 'd2f33060-6988-4e03-a481-439d10aabc83';

-- COMMAND ----------

describe history encounters_cdc_source

-- COMMAND ----------

-- DBTITLE 1,Get the difference between the two table versions using "except" and save as a temp view
CREATE OR REPLACE TEMPORARY VIEW encounters_updates
AS 
SELECT * FROM encounters_cdc_source except select * from encounters_cdc_source version as of 14 -- check for version in previous cell block!!!

-- COMMAND ----------

select * from encounters_updates

-- COMMAND ----------

-- DBTITLE 1,Merge the changes into gold
MERGE INTO diff_cdc_sink
USING encounters_updates
ON diff_cdc_sink.PK = encounters_updates.PK
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED
  THEN INSERT *

-- COMMAND ----------


