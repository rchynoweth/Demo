-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Change Data Feed with Databricks Delta
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
-- MAGIC We will demonstrate best practices around capturing change data using the Change Data Feed capabilities of Delta Lake. 
-- MAGIC 
-- MAGIC To show this process we will `set spark.databricks.delta.properties.defaults.enableChangeDataCapture = True` where this feature will be enabled by default for all Delta tables created in our applicatioin run. Then we will create a silver delta table and a gold delta table that is created from the silver data. We will then insert data, update data, and delete data in our silver table. Using the change data feed feature we will capture the most recent changes for all rows and merge them into our gold table in a single command! 
-- MAGIC 
-- MAGIC In short, we are able to capture the changes between versions of a delta table and execute a single command to merge those changes into other tables. 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## How does Delta CDF Work? 
-- MAGIC 
-- MAGIC 1. We start with the original table, which has three records (A1 through A3) and two fields (PK and B)
-- MAGIC 1. We then receive an updated version of this table that shows a change to field B in record A2, removal of record A3, and the addition of record A4
-- MAGIC 1. As this gets processed, the Delta Change Data Feed captures only records for which there was a change. This is what allows Delta Change Data Feed to speed up ETL pipelines as less data is being touched. 
-- MAGIC 1. Note that record A1 is not reflected in the Change Data Feed output as no changes were made to that record.
-- MAGIC 1. In the case of updates, the output contains what the record looked like prior to the change, called the preimage, and what it held after the change, called the postimage. This can be particularly helpful when producing aggregated facts or materialized views as appropriate updates could be made to individual records without having to reprocess all of the data underlying the table. This allows changes to be reflected more quickly in the data used for BI and visualization.
-- MAGIC 1. For deletes and inserts, the affected record is shown with an indication of whether it is being added or removed.
-- MAGIC 1. Additionally, the Delta version is noted so that logs are maintained of what happened to the data when. This allows greater granularity for regulatory and audit purposes as needed. We do capture the timestamp of these commits as well and will show this in a few minutes.
-- MAGIC 1. While this depicts a batch process, the source of updates could just as easily by a stream instead. The Change Data Feed output would be the same.
-- MAGIC 
-- MAGIC 
-- MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/Delta CDF Log Visual.png"/>
-- MAGIC <br></br>
-- MAGIC <br></br>
-- MAGIC 
-- MAGIC ## Where to use Delta Change Data Feed?
-- MAGIC 
-- MAGIC Delta Change Data Feed is an output from Delta Lake. This means that CDF applies when processing from a Delta format to a further step in the data flow.  
-- MAGIC 
-- MAGIC In order to take advantage of the Delta Change Data Feed, simply bring in your external data sources to the Bronze layer, as previously done, and then enable CDF from that point forward.  
-- MAGIC 
-- MAGIC This will allow you to use the Change Data Feed in moving to the Silver or Gold layers or feeding out to an external platform.  
-- MAGIC 
-- MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/Delta CDF Use Case.png" />

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # this database should be the same as the same as the set up notebooks 
-- MAGIC dbutils.widgets.text("database_name", "demo_omop_database")
-- MAGIC spark.sql("CREATE DATABASE IF NOT EXISTS {} LOCATION '/FileStore/tables/{}'".format(dbutils.widgets.get("database_name"), dbutils.widgets.get("database_name")))
-- MAGIC spark.sql("use {}".format(dbutils.widgets.get("database_name"))) ## select the database to use should be the same as the notebooks 1-3 for data setup

-- COMMAND ----------

-- drop tables if they exist
drop table if exists encounters_cdc_source;
drop table if exists delta_cdc_sink;
drop table if exists watermark_metadata;

-- COMMAND ----------

-- spark configuration for delta change data capture
-- This will auto set this for all tables created in this session
set spark.databricks.delta.properties.defaults.enableChangeDataCapture = True

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Alternatively you can run `ALTER TABLE` commands to enable on a per table basis:
-- MAGIC ```sql
-- MAGIC ALTER TABLE ... 
-- MAGIC SET TBLPROPERTIES (delta.enableChangeDataCapture = true);
-- MAGIC ```

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
-- MAGIC 
-- MAGIC display(df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Create a delta table to use for cdc
-- MAGIC df.write.format("delta").saveAsTable("encounters_cdc_source")

-- COMMAND ----------

-- DBTITLE 1,Let's take a look at our dataset and view the history
select * from encounters_cdc_source

-- COMMAND ----------

describe history encounters_cdc_source

-- COMMAND ----------

-- DBTITLE 1,We execute updates, deletes, and inserts and take a look at what that does to table history
UPDATE encounters_cdc_source set STOP = current_timestamp() where PATIENT = 'a45a0f84-1d16-4517-9aa8-271c59f339fd'

-- COMMAND ----------

ALTER TABLE encounters_cdc_source ADD COLUMN (flag boolean)

-- COMMAND ----------

describe history encounters_cdc_source

-- COMMAND ----------

SELECT * FROM encounters_cdc_source version as of 1

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
SELECT * FROM table_changes('encounters_cdc_source', 1, 3)


-- COMMAND ----------

-- DBTITLE 1,Insert a single row
-- MAGIC %python
-- MAGIC max_id = spark.sql("select max(pk) from encounters_cdc_source").collect()[0][0]
-- MAGIC spark.sql("INSERT INTO encounters_cdc_source values (current_timestamp(), current_timestamp(), 'inserted_row', 'inserted_row', 123456, 'description', {})".format(max_id))

-- COMMAND ----------

-- table changes since version 4
SELECT * FROM table_changes('encounters_cdc_source', 4)


-- COMMAND ----------

-- DBTITLE 1,Delete some rows 
DELETE FROM encounters_cdc_source where PATIENT = 'cc3e87e4-6172-45cb-81d2-9b5209617e00'

-- COMMAND ----------

-- table changes since version 4
SELECT * FROM table_changes('encounters_cdc_source', 5)

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

-- MAGIC %md ## Delta CDC gives back 4 cdc types in the "__cdc_type" column:  
-- MAGIC 
-- MAGIC | Change Type             | Description                                                               |
-- MAGIC |----------------------|---------------------------------------------------------------------------|
-- MAGIC | **update_preimage**  | Content of the row before an update                                       |
-- MAGIC | **update_postimage** | Content of the row after the update (what you want to capture downstream) |
-- MAGIC | **delete**           | Content of a row that has been deleted                                    |
-- MAGIC | **insert**           | Content of a new row that has been inserted                               |
-- MAGIC 
-- MAGIC Therefore, 1 update will result in 2 rows in the cdc stream (one row with the previous values, one with the new values)

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
      , _change_type

      FROM (
        SELECT *, 

        dense_rank() OVER (PARTITION BY PK ORDER BY _commit_version DESC) as rank


        FROM table_changes('encounters_cdc_source', 1) 

        WHERE _change_type != 'update_preimage'
      ) 
      WHERE rank = 1


-- COMMAND ----------

-- these are our cdc changes
select * from encounters_updates

-- COMMAND ----------

-- DBTITLE 1,Merge the changes into our gold table

MERGE INTO delta_cdc_sink

USING encounters_updates AS source
  ON delta_cdc_sink.PK = source.PK
  
WHEN MATCHED AND source._change_type = 'delete' THEN DELETE

WHEN MATCHED AND source._change_type = 'update_postimage' THEN 
  UPDATE SET START=source.start, STOP=source.stop, PATIENT=source.patient, ENCOUNTER=source.encounter, CODE=source.code, DESCRIPTION=source.description, PK=source.pk
  
WHEN NOT MATCHED AND source._change_type = 'insert' THEN 
  INSERT (START, STOP, PATIENT, ENCOUNTER, CODE, DESCRIPTION, PK) 
  VALUES (source.start, source.stop, source.patient, source.encounter, source.code, source.description, source.pk)

-- COMMAND ----------

SELECT * FROM delta_cdc_sink

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Recommended Best Practice**
-- MAGIC 
-- MAGIC In order to implement change data feed as your CDC solution in production, you will need to maintain watermark files containing the version or datetime that the application last read from the source delta table. There are two options:
-- MAGIC 1. Maintain a watermark metadata table containing the sink table name, path, and version/datetime (recommended option)
-- MAGIC 1. Maintain a log file on DBFS underneath the sink delta table
-- MAGIC   - Best practice would be to save this as a text file using `_watermark` so that spark will ignore this directory when you are reading/writing to the table
-- MAGIC 
-- MAGIC Note that with change data feed you supply the versions when you **READ** the source delta table, but you will want to maintain your watermark when you **WRITE** to the sink delta table.  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC We have processed all the changes from our silver table into our gold table. We will want to save the latest version of the silver that we have processed to a watermark delta table. 
-- MAGIC 
-- MAGIC Now we will:
-- MAGIC 1. Create our metadata table
-- MAGIC 1. Select the max version from our silver history 
-- MAGIC 1. Insert a row into our metadata table
-- MAGIC 1. Execute a single change 
-- MAGIC 1. Recreate our cdc view from command 24
-- MAGIC 1. Re-execute our merge

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS watermark_metadata 
(table_name STRING,
cdc_version INTEGER )
USING DELTA ;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC version = spark.sql(""" select max(h.version)
-- MAGIC   from (describe history encounters_cdc_source) as h
-- MAGIC   limit 1 """).collect()[0][0]
-- MAGIC 
-- MAGIC 
-- MAGIC spark.sql("""
-- MAGIC INSERT INTO watermark_metadata (table_name, cdc_version) values ('delta_cdc_sink', {})
-- MAGIC """.format(version))

-- COMMAND ----------

select * from watermark_metadata

-- COMMAND ----------

delete from encounters_cdc_source where PATIENT = '80f3ad76-d17c-46a4-b23d-b978bc394808'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC version = spark.sql("select cdc_version+1 from watermark_metadata where table_name='delta_cdc_sink' limit 1").collect()[0][0]
-- MAGIC print("Version: {}".format(version)) # we do +1 because we are saving the last version processed so it would be the next version we want
-- MAGIC 
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
-- MAGIC       , _change_type
-- MAGIC 
-- MAGIC       FROM (
-- MAGIC         SELECT *, 
-- MAGIC 
-- MAGIC         dense_rank() OVER (PARTITION BY PK ORDER BY _commit_version DESC) as rank
-- MAGIC 
-- MAGIC 
-- MAGIC         FROM table_changes('encounters_cdc_source', {}) 
-- MAGIC 
-- MAGIC         WHERE _change_type != 'update_preimage'
-- MAGIC       ) 
-- MAGIC       WHERE rank = 1
-- MAGIC """.format(version))

-- COMMAND ----------


MERGE INTO delta_cdc_sink

USING encounters_updates AS source
  ON delta_cdc_sink.PK = source.PK
  
WHEN MATCHED AND source._change_type = 'delete' THEN DELETE

WHEN MATCHED AND source._change_type = 'update_postimage' THEN 
  UPDATE SET START=source.start, STOP=source.stop, PATIENT=source.patient, ENCOUNTER=source.encounter, CODE=source.code, DESCRIPTION=source.description, PK=source.pk
  
WHEN NOT MATCHED AND source._change_type = 'insert' THEN 
  INSERT (START, STOP, PATIENT, ENCOUNTER, CODE, DESCRIPTION, PK) 
  VALUES (source.start, source.stop, source.patient, source.encounter, source.code, source.description, source.pk)

-- COMMAND ----------

UPDATE encounters_cdc_source set STOP = current_timestamp() where PATIENT = 'a45a0f84-1d16-4517-9aa8-271c59f339fd' -- this is the same patient as the first update 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ## update our metadata!!
-- MAGIC version = spark.sql(""" select max(h.version)
-- MAGIC   from (describe history encounters_cdc_source) as h
-- MAGIC   limit 1 """).collect()[0][0]
-- MAGIC 
-- MAGIC 
-- MAGIC spark.sql("""
-- MAGIC UPDATE watermark_metadata SET cdc_version={} WHERE table_name='delta_cdc_sink'
-- MAGIC """.format(version))

-- COMMAND ----------

select * from watermark_metadata

-- COMMAND ----------

-- MAGIC %md
-- MAGIC What just happened? 
-- MAGIC - Created a silver Databricks Delta table
-- MAGIC - Executed updates, inserts, and delete commands on the table
-- MAGIC - Created a gold Databricks Delta table that was the exact same as the unaltered version of our silver table
-- MAGIC - Selected the changes and merged them into our gold Delta table 
-- MAGIC - Created a metadata table to store our version watermarking
-- MAGIC - Executed a delete command against our silver table
-- MAGIC - Used our watermark to only get the deletion changes and merge into our gold table
-- MAGIC - Updated our watermark table

-- COMMAND ----------


