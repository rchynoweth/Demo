# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/databricks icon.png?raw=true" width=100/> 
# MAGIC # Delta Introduction
# MAGIC 
# MAGIC In this demonstration we will deep dive into how delta works and how to work with delta for all your big data pipelines. 
# MAGIC 
# MAGIC #### Databricks' Delta Lake is the world's most advanced data lake technology.  
# MAGIC 
# MAGIC Delta Lake brings __*Performance*__ and __*Reliability*__ to Data Lakes
# MAGIC 
# MAGIC 
# MAGIC ###Why Delta Lake?<br><br>
# MAGIC 
# MAGIC <div style="img align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://user-images.githubusercontent.com/20408077/87175470-4d8e1580-c29e-11ea-8f33-0ee14348a2c1.png" width="500"/>
# MAGIC </div>
# MAGIC 
# MAGIC At a glance, Delta Lake is an open source storage layer that brings both **reliability and performance** to data lakes. Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing. 
# MAGIC 
# MAGIC Delta Lake runs on top of your existing data lake and is fully compatible with Apache Spark APIs. [For more information](https://docs.databricks.com/delta/delta-intro.html)

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "")
dbutils.widgets.text("UserName", "")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

database_name = dbutils.widgets.get("DatabaseName")
user_name = dbutils.widgets.get("UserName")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")

# COMMAND ----------

spark.sql(f"USE {database_name}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- spark configuration for delta change data capture
# MAGIC -- This will auto set this for ALL tables created in this session
# MAGIC -- this can also be done on individual tables
# MAGIC set spark.databricks.delta.properties.defaults.enableChangeDataCapture = True

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingesting Data from DBFS
# MAGIC 
# MAGIC The use case we will be demonstrating here illustrates the "Bronze-Silver-Gold" paradigm which is a best practice for data lakes.
# MAGIC 
# MAGIC - We ingest data as soon as we can into the lake, even though we know it may need cleansing or enrichment.  This gives us a baseline of the freshest possible data for exploration.  We call this the __Bronze__ version of the data.
# MAGIC 
# MAGIC - We then cleanse and enrich the Bronze data, creating a "single version of truth" that we call the __Silver__ version.
# MAGIC 
# MAGIC - From the Silver data, we can generate many __Gold__ versions of the data.  Gold versions are typically project-specific, and typically filter, aggregate, and re-format Silver data to make it easy to use in specific projects.
# MAGIC 
# MAGIC We'll read the raw data into a __Dataframe__.  The dataframe is a key structure in Apache Spark.  It is an in-memory data structure in a rows-and-columns format that is very similar to a relational database table.  In fact, we'll be creating SQL Views against the dataframes so that we can manipulate them using standard SQL. 
# MAGIC 
# MAGIC As an alternative pure SQL option we could have used the [`COPY INTO`](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-copy-into.html) (which is one of my favorite ways to ingest data).   

# COMMAND ----------



# COMMAND ----------

spark.sql("drop table if exists temp_device_delta")
spark.sql("drop table if exists temp_device_delta_edits")

# load and save data as delta tables
device_df = (spark.read.format("json").load("/databricks-datasets/iot-stream/data-device/*.json.gz")).drop("value")
device_df.write.format("delta").saveAsTable("temp_device_delta")
device_df.write.format("delta").saveAsTable("temp_device_delta_edits")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- we can use SQL to access data through the data catalog
# MAGIC SELECT * FROM temp_device_delta

# COMMAND ----------

# DBTITLE 1,We will intentionally create dirty data to show off delta capabilities 
# lets create negative calorie burn values for some rows
spark.sql("drop table if exists temp_device_delta_bad")
spark.sql("drop table if exists temp_device_delta_bad2")
device_df_bad = device_df.limit(1000).withColumn("calories_burnt", lit(-90000)).drop("value")
device_df_bad.write.format("delta").saveAsTable("temp_device_delta_bad")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_device_delta_bad

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create some more dirty data to change some rows to have negative num_steps
# MAGIC CREATE TABLE temp_device_delta_bad2 AS 
# MAGIC SELECT calories_burnt, 
# MAGIC   device_id, 
# MAGIC   id, 
# MAGIC   miles_walked, 
# MAGIC   -1 as num_steps, 
# MAGIC   timestamp, 
# MAGIC   user_id 
# MAGIC FROM temp_device_delta
# MAGIC WHERE id not in (SELECT id from temp_device_delta_bad) 
# MAGIC LIMIT 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_device_delta_bad2

# COMMAND ----------

# MAGIC %md
# MAGIC We dirtied some data so that we can show off our merge capabilities! We now have "dirty" data in our **managed** table within databricks 

# COMMAND ----------

# DBTITLE 1,Let's do a merge to add the dirty data to our bronze table
# MAGIC %sql
# MAGIC MERGE INTO temp_device_delta as target 
# MAGIC USING temp_device_delta_bad as source 
# MAGIC ON target.id = source.id 
# MAGIC WHEN MATCHED AND source.calories_burnt = -90000 THEN UPDATE SET * ; 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_device_delta where calories_burnt = -90000

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO temp_device_delta as target 
# MAGIC USING temp_device_delta_bad2 as source 
# MAGIC ON target.id = source.id 
# MAGIC WHEN MATCHED THEN UPDATE SET * ; 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM temp_device_delta where num_steps = -1

# COMMAND ----------

# DBTITLE 1,Syntax to convert an existing parquet dataset to a delta table
# MAGIC %sql
# MAGIC -- CONVERT TO DELTA [ table_identifier | parquet.`<path-to-table>` ] [NO STATISTICS]
# MAGIC -- [PARTITIONED BY (col_name1 col_type1, col_name2 col_type2, ...)]

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sum(miles_walked) as summed_miles, user_id
# MAGIC FROM temp_device_delta
# MAGIC GROUP BY user_id
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Silver table
# MAGIC 
# MAGIC Okay so we have created a **bronze** table and some of the data is dirty. Let's write some code to fix the data and write it to a **silver** table.  

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's create a Silver table.  We'll start with the Bronze data, then make several improvements
# MAGIC 
# MAGIC DROP TABLE IF EXISTS iot_readings_silver;
# MAGIC 
# MAGIC CREATE TABLE iot_readings_silver  
# MAGIC using Delta
# MAGIC as select * from temp_device_delta

# COMMAND ----------

# DBTITLE 1,How is there people who burned -90,000 calories???
# MAGIC %sql
# MAGIC SELECT * FROM iot_readings_silver where calories_burnt=-90000

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's merge in the Bronze backfill data
# MAGIC -- MERGE INTO is one of the most important differentiators for Delta Lake
# MAGIC -- The entire backfill batch will be treated as an atomic transaction,
# MAGIC -- and we can do both inserts and updates within a single batch.
# MAGIC 
# MAGIC MERGE INTO iot_readings_silver as target 
# MAGIC USING temp_device_delta_edits as source 
# MAGIC ON target.id = source.id 
# MAGIC WHEN MATCHED AND target.calories_burnt = -90000 THEN UPDATE SET *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- there should be no outputs
# MAGIC SELECT * FROM iot_readings_silver where calories_burnt=-90000

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Spark SQL 
# MAGIC 
# MAGIC Spark SQL is a very mature language that is ANSI SQL Compliant and has many of the functions available in other SQL variations. So lets look into updating some bad data by using some window functions. We want to replace the values of -1 with local averages. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM iot_readings_silver
# MAGIC WHERE num_steps = -1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We'll create a table of these interpolated readings, then later we'll merge it into the Silver table.
# MAGIC 
# MAGIC DROP TABLE IF EXISTS iot_readings_intermediate;
# MAGIC 
# MAGIC CREATE TABLE iot_readings_intermediate AS (
# MAGIC   WITH lags_and_leads AS (
# MAGIC SELECT
# MAGIC       id, 
# MAGIC       user_id,
# MAGIC       calories_burnt,
# MAGIC       miles_walked,
# MAGIC       device_id,
# MAGIC       timestamp,
# MAGIC       num_steps, 
# MAGIC       LAG(num_steps, 1, 0)  OVER (PARTITION BY device_id ORDER BY timestamp ASC, device_id ASC) AS num_steps_lag,
# MAGIC       LEAD(num_steps, 1, 0) OVER (PARTITION BY device_id ORDER BY timestamp ASC, device_id ASC) AS num_steps_lead
# MAGIC     FROM iot_readings_silver
# MAGIC   )
# MAGIC   SELECT 
# MAGIC       id, 
# MAGIC       user_id,
# MAGIC       calories_burnt,
# MAGIC       miles_walked,
# MAGIC       device_id,
# MAGIC       timestamp,
# MAGIC     ((num_steps_lag + num_steps_lead) / 2) AS num_steps
# MAGIC     
# MAGIC   FROM lags_and_leads
# MAGIC   WHERE num_steps=-1
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lets take a look at the data we fixed 
# MAGIC SELECT * FROM iot_readings_intermediate

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Lets merge the tables together so that we can fix those 999.99 values
# MAGIC MERGE INTO iot_readings_silver AS target 
# MAGIC USING iot_readings_intermediate AS source 
# MAGIC ON target.id = source.id
# MAGIC 
# MAGIC WHEN MATCHED THEN UPDATE SET * 
# MAGIC WHEN NOT MATCHED THEN INSERT * 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now make sure we got rid of all the bogus steps.
# MAGIC SELECT * 
# MAGIC FROM iot_readings_silver
# MAGIC WHERE num_steps = -1

# COMMAND ----------

# DBTITLE 1,Inserts, updates, and deletes!
# MAGIC %sql
# MAGIC INSERT INTO iot_readings_silver VALUES (200, 5, 2000000, 1.59, 10000, '2021-01-01 21:12:34.123456', 16);
# MAGIC UPDATE iot_readings_silver SET num_steps=12000 WHERE user_id=33;
# MAGIC DELETE FROM iot_readings_silver WHERE user_id=22;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iot_readings_silver WHERE id = 2000000;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iot_readings_silver WHERE user_id=33;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iot_readings_silver WHERE user_id=22;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time travel
# MAGIC 
# MAGIC We messed up our data! Let's use time travel to roll back the changes to version 2 of our table, as that was the last time our data was clean. 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all the versions of the table that are available to us
# MAGIC 
# MAGIC DESCRIBE HISTORY iot_readings_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM iot_readings 
# MAGIC VERSION AS OF 2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE iot_readings_silver AS 
# MAGIC SELECT * FROM iot_readings_silver
# MAGIC VERSION AS OF 2

# COMMAND ----------

# DBTITLE 1,The user we added is gone!
# MAGIC %sql
# MAGIC -- you can write queries to test the other changes we made too if you want
# MAGIC SELECT * FROM iot_readings_silver 
# MAGIC WHERE user_id = 2000000

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY iot_readings_silver

# COMMAND ----------

# DBTITLE 1,Check out some of the metadata about our table
# MAGIC %sql
# MAGIC DESCRIBE EXTENDED iot_readings_silver

# COMMAND ----------

# MAGIC %md
# MAGIC **What does the delta table look like under the hood?**

# COMMAND ----------

dbutils.fs.ls(f"dbfs:/user/hive/warehouse/{database_name}.db/iot_readings")

# As you can see, the data is just broken into a set of files, without regard to the meaning of the data
# the "_delta_log" is where the magic happens

# COMMAND ----------

# DBTITLE 1,Optimizing our table for faster reads
# MAGIC %sql
# MAGIC -- optimize compacts our table to avoid small files
# MAGIC -- ZORDER is an index for data skipping 
# MAGIC OPTIMIZE iot_readings_silver ZORDER BY timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### Change Data Feed
# MAGIC 
# MAGIC CDF is a CDC mechanism that allows for effecient CDC operations between versions of a delta table. 
# MAGIC 
# MAGIC The command below gets a result set of all the changes that occured between versions 1 and 2 of the table. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('iot_readings_silver', 1, 2)

# COMMAND ----------

# MAGIC %md ## Delta CDC gives back 4 cdc types in the "__cdc_type" column:  
# MAGIC 
# MAGIC | Change Type             | Description                                                               |
# MAGIC |----------------------|---------------------------------------------------------------------------|
# MAGIC | **update_preimage**  | Content of the row before an update                                       |
# MAGIC | **update_postimage** | Content of the row after the update (what you want to capture downstream) |
# MAGIC | **delete**           | Content of a row that has been deleted                                    |
# MAGIC | **insert**           | Content of a new row that has been inserted                               |
# MAGIC 
# MAGIC Therefore, 1 update will result in 2 rows in the cdc stream (one row with the previous values, one with the new values)
# MAGIC 
# MAGIC 
# MAGIC The command below will get the most recent version of each record from our change records. This result set can then be merged into downstream tables 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now I want to select the changes between version 0 and version 1 and update my gold table
# MAGIC -- first let's create a temp view
# MAGIC 
# MAGIC CREATE OR REPLACE TEMPORARY VIEW iot_readings_cdc
# MAGIC     AS 
# MAGIC     SELECT 
# MAGIC       calories_burnt
# MAGIC       , device_id
# MAGIC       , id
# MAGIC       , miles_walked
# MAGIC       , num_steps
# MAGIC       , timestamp
# MAGIC       , user_id
# MAGIC       , _change_type
# MAGIC       , rank
# MAGIC       , _commit_version
# MAGIC 
# MAGIC       FROM (
# MAGIC         SELECT *, 
# MAGIC 
# MAGIC         dense_rank() OVER (PARTITION BY id ORDER BY _commit_version DESC) as rank
# MAGIC 
# MAGIC 
# MAGIC         FROM table_changes('iot_readings_silver', 4,5) 
# MAGIC 
# MAGIC         WHERE _change_type != 'update_preimage'
# MAGIC       ) 
# MAGIC       WHERE rank = 1
# MAGIC ;
# MAGIC 
# MAGIC SELECT * FROM iot_readings_cdc

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE iot_readings_gold AS 
# MAGIC 
# MAGIC SELECT * FROM iot_readings_silver

# COMMAND ----------

# DBTITLE 1,We can merge data into downstream tables with the following command
# MAGIC %sql
# MAGIC -- this command handles updates, deletes, and inserts!!
# MAGIC 
# MAGIC 
# MAGIC MERGE INTO iot_readings_gold
# MAGIC 
# MAGIC USING iot_readings_cdc AS source
# MAGIC   ON iot_readings_gold.id = source.id
# MAGIC   
# MAGIC WHEN MATCHED AND source._change_type = 'delete' THEN DELETE
# MAGIC 
# MAGIC WHEN MATCHED AND source._change_type = 'update_postimage' THEN 
# MAGIC   UPDATE SET calories_burnt=source.calories_burnt, device_id=source.device_id, id=source.id, miles_walked=source.miles_walked, num_steps=source.num_steps, timestamp=source.timestamp, user_id=source.user_id
# MAGIC   
# MAGIC WHEN NOT MATCHED AND source._change_type = 'insert' THEN 
# MAGIC   INSERT ( calories_burnt, device_id, id, miles_walked, num_steps, timestamp, user_id) 
# MAGIC   VALUES (source.calories_burnt, source.device_id, source.id, source.miles_walked, source.num_steps, source.timestamp, source.user_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM iot_readings_gold

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC That's it! A quick "gentle" introduction to Delta lake. 
# MAGIC 
# MAGIC If you are interested in more delta lake then you should check out [Spark Streaming](https://docs.databricks.com/delta/delta-streaming.html) with Delta Lake. Everything we did in this notebook was batch processing, but the same tables can be streaming.  

# COMMAND ----------


