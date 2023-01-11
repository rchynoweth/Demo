# Databricks notebook source
dbutils.widgets.text("DatabaseName", "rac_demo_db")
dbutils.widgets.text("UserName", "ryan.chynoweth@databricks.com")
database_name = dbutils.widgets.get("DatabaseName")
user_name = dbutils.widgets.get("UserName")
spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
spark.sql("USE {}".format(database_name))

# COMMAND ----------

setup_responses = dbutils.notebook.run("./includes/1_setup", 0, {"database_name": database_name, "user_name": user_name}).split()

local_data_path = setup_responses[0]
dbfs_data_path = setup_responses[1]
database_name = setup_responses[2]

print(f"Path to be used for Local Files: {local_data_path}")
print(f"Path to be used for DBFS Files: {dbfs_data_path}")
print(f"Database Name: {database_name}")

# COMMAND ----------

# Let's set the default database name so we don't have to specify it on every query

spark.sql(f"USE {database_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bringing data into a dataframe
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

# COMMAND ----------

# Read the downloaded historical data into a dataframe
# This is MegaCorp data regarding power plant device performance.  It pre-dates our new IOT effort, but we want to save this data and use it in queries.

#location and file type
dataPath = dbfs_data_path+"/bronze/Lakehouse_ingest.csv"
file_type = "csv"

#options
infer_schema = "true"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(dataPath)

display(df)

# COMMAND ----------

df.createOrReplaceTempView("tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct device_operational_status
# MAGIC FROM tmp

# COMMAND ----------

# Read the downloaded backfill data into a dataframe
# This is some backfill data that we'll need to merge into the main historical data.  

dataPath = dbfs_data_path+"/bronze/Lakehouse_ingest_backfill.csv"

df_backfill = (spark
               .read
               .format("csv")
               .option("inferSchema", "true")
               .option("header", "true")
               .load(dataPath))


display(df_backfill)

# COMMAND ----------

# DBTITLE 1,Create temporary views of our dataframes 
# Create a temporary view on the dataframes to enable SQL

df.createOrReplaceTempView("historical_bronze_vw")
df_backfill.createOrReplaceTempView("historical_bronze_backfill_vw")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Bronze Historical Tables
# MAGIC 
# MAGIC 
# MAGIC __*Delta Lake was built to solve these reliability and performance problems.*__  First, let's consider how Delta Lake addresses *reliability* issues...
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/delta_reliability.png?raw=true" width=1000/>
# MAGIC 
# MAGIC Note the Key Features in the graphic above.  We'll be diving into all of these capabilities as we go through the Workshop:
# MAGIC 
# MAGIC - __ACID Transactions:__ Delta Lake ACID compliance ensures that half-completed transactions are never persisted in the Lake, and concurrent users never see other users' in-flight transactions.
# MAGIC 
# MAGIC - __Mutations:__ Experienced relational database architects may be surprised to learn that most data lakes do not support updates and deletes.  These lakes concern themselves only with data ingest, which makes error correction and backfill very difficult.  In contrast, Delta Lake provides full support for Inserts, Updates, and Deletes.
# MAGIC 
# MAGIC - __Schema Enforcement:__ Delta Lake provides full support for schema enforcement at write time, greatly increasing data reliability.
# MAGIC 
# MAGIC - __Unified Batch and Streaming:__ Streaming data is becoming an essential capability for all enterprises.  We'll see how Delta Lake supports both batch and streaming modes, and in fact blurs the line between them, enabling architects to design systems that use both batch and streaming capabilities simultaneously.
# MAGIC 
# MAGIC - __Time Travel:__ unlike most data lakes, Delta Lake enables queries of data *as it existed* at a specific point in time.  This has important ramifications for reliability, error recovery, and synchronization with other systems, as we shall see later in this Workshop.
# MAGIC 
# MAGIC We have seen how Delta Lake enhances reliability.  Next, let's see how Delta Lake optimizes __*performance*__...
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/delta_performance.png?raw=true" width=1000/>
# MAGIC 
# MAGIC Again, we'll be diving into all these capabilities throughout the Workshop.  We'll be concentrating especially on features that are only available in Databricks' distribution of Delta Lake...
# MAGIC 
# MAGIC - __Compaction:__ Delta Lake provides sophisticated capabilities to solve the "small-file problem" by compacting small files into larger units.
# MAGIC 
# MAGIC - __Caching:__ Delta Lake transparently caches data on the SSD drives of worker nodes in a Spark cluster, greatly improving performance.
# MAGIC 
# MAGIC - __Data Skipping:__ this Delta Lake feature goes far beyond the limits of mere partitioning.
# MAGIC 
# MAGIC - __Z-Ordering:__ this is a brilliant alternative to traditional indexing, and further enhances Delta Lake performance.
# MAGIC 
# MAGIC Now that we have introduced the value proposition of Delta Lake, let's get a deeper understanding of the overall "Data Lake" concept.

# COMMAND ----------

# DBTITLE 1,Create a bronze delta table
spark.sql("DROP TABLE IF EXISTS sensor_readings_historical_bronze;")

spark.sql(
"""
  CREATE TABLE sensor_readings_historical_bronze
  Using Delta
  as Select * from historical_bronze_vw;
"""
)



# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's take a peek at our new bronze table
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_historical_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's count the records in the Bronze table
# MAGIC 
# MAGIC SELECT COUNT(*) FROM sensor_readings_historical_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's make a query that shows a meaningful graphical view of the table
# MAGIC -- How many rows exist for each operational status?
# MAGIC -- Experiment with different graphical views... be creative!
# MAGIC 
# MAGIC SELECT count(*) as status_count, device_operational_status
# MAGIC FROM sensor_readings_historical_bronze
# MAGIC GROUP BY device_operational_status
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's take a peek at the backfill data
# MAGIC 
# MAGIC SELECT * FROM historical_bronze_backfill_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's count the records in the backfill data
# MAGIC 
# MAGIC SELECT COUNT(*) FROM historical_bronze_backfill_vw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Silver table
# MAGIC 
# MAGIC The bronze dataset has a couple of issues so we will want to do a couple of transformations in order to prepare it for our machine learning model. 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's create a Silver table.  We'll start with the Bronze data, then make several improvements
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_silver;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_silver 
# MAGIC using Delta
# MAGIC as select * from historical_bronze_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's take a peek at our new Silver table
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_historical_silver
# MAGIC ORDER BY reading_time ASC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's merge in the Bronze backfill data
# MAGIC -- MERGE INTO is one of the most important differentiators for Delta Lake
# MAGIC -- The entire backfill batch will be treated as an atomic transaction,
# MAGIC -- and we can do both inserts and updates within a single batch.
# MAGIC 
# MAGIC MERGE INTO sensor_readings_historical_silver AS target 
# MAGIC USING historical_bronze_backfill_vw AS source 
# MAGIC ON target.id = source.id
# MAGIC WHEN MATCHED THEN UPDATE SET * 
# MAGIC WHEN NOT MATCHED THEN INSERT * 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify that the upserts worked correctly.
# MAGIC -- Newly inserted records have dates of 2015-02-21 (and id value beginning with 'ZZZ')
# MAGIC -- Updated records have id's in the backfill data that do NOT begin with 'ZZZ'.  
# MAGIC -- Check a few of these, and make sure that a tiny value was added to reading_1.
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_historical_silver
# MAGIC ORDER BY reading_time ASC

# COMMAND ----------

# DBTITLE 1,Looks like we have more messy data "999.99"
# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver
# MAGIC WHERE reading_1 = 999.99

# COMMAND ----------

# DBTITLE 1,Replace the bad readings with the average of the previous and following readings
# MAGIC %sql
# MAGIC -- We want to fix these bogus readings.  Here's the idea...
# MAGIC -- - Use a SQL window function to order the readings by time within each device
# MAGIC -- - Whenever there is a 999.99 reading, replace it with the AVERAGE of the PREVIOUS and FOLLOWING readings.
# MAGIC -- Solution:
# MAGIC -- Window functions use an "OVER" clause... OVER (PARTITION BY ... ORDER BY )
# MAGIC -- Look up the doc for SQL functions LAG() and LEAD()
# MAGIC 
# MAGIC -- We'll create a table of these interpolated readings, then later we'll merge it into the Silver table.
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_interpolations;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_interpolations AS (
# MAGIC   WITH lags_and_leads AS (
# MAGIC     SELECT
# MAGIC       id, 
# MAGIC       reading_time,
# MAGIC       device_type,
# MAGIC       device_id,
# MAGIC       device_operational_status,
# MAGIC       reading_1,
# MAGIC       LAG(reading_1, 1, 0)  OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_1_lag,
# MAGIC       LEAD(reading_1, 1, 0) OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_1_lead,
# MAGIC       reading_2,
# MAGIC       LAG(reading_2, 1, 0)  OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_2_lag,
# MAGIC       LEAD(reading_2, 1, 0) OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_2_lead,
# MAGIC       reading_3,
# MAGIC       LAG(reading_3, 1, 0)  OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_3_lag,
# MAGIC       LEAD(reading_3, 1, 0) OVER (PARTITION BY device_id ORDER BY reading_time ASC, id ASC) AS reading_3_lead
# MAGIC     FROM sensor_readings_historical_silver
# MAGIC   )
# MAGIC   SELECT 
# MAGIC     id,
# MAGIC     reading_time,
# MAGIC     device_type,
# MAGIC     device_id,
# MAGIC     device_operational_status,
# MAGIC     ((reading_1_lag + reading_1_lead) / 2) AS reading_1,
# MAGIC     ((reading_2_lag + reading_2_lead) / 2) AS reading_2,
# MAGIC     ((reading_3_lag + reading_3_lead) / 2) AS reading_3
# MAGIC   FROM lags_and_leads
# MAGIC   WHERE reading_1 = 999.99
# MAGIC   ORDER BY id ASC
# MAGIC )

# COMMAND ----------

# DBTITLE 1,The readings have been fixed
# MAGIC %sql
# MAGIC -- Let's examine our interpolations to make sure they are correct
# MAGIC 
# MAGIC SELECT * FROM sensor_readings_historical_interpolations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's see how many interpolations we have.  There should be 367 rows.
# MAGIC 
# MAGIC SELECT COUNT(*) FROM sensor_readings_historical_interpolations

# COMMAND ----------

# DBTITLE 1,Fix the rows with data errors
# MAGIC %sql
# MAGIC -- Now use MERGE INTO to update the historical table
# MAGIC 
# MAGIC MERGE INTO sensor_readings_historical_silver AS target 
# MAGIC USING sensor_readings_historical_interpolations AS source 
# MAGIC ON target.id = source.id
# MAGIC 
# MAGIC WHEN MATCHED THEN UPDATE SET * 
# MAGIC WHEN NOT MATCHED THEN INSERT * 

# COMMAND ----------

# DBTITLE 1,Should return no data
# MAGIC %sql
# MAGIC -- Now make sure we got rid of all the bogus readings.
# MAGIC -- Gee, this is fast.  Why?  What feature in Delta Lake is making this so speedy?
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver
# MAGIC WHERE reading_1 = 999.99

# COMMAND ----------

# MAGIC %md
# MAGIC Now we've lost visibility into which readings were initially faulty... use Time Travel to recover this information.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all the versions of the table that are available to us
# MAGIC 
# MAGIC DESCRIBE HISTORY sensor_readings_historical_silver

# COMMAND ----------

# DBTITLE 1,Time Travel
# MAGIC %sql
# MAGIC -- Ah, version 1 should have the 999.99 values
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver 
# MAGIC VERSION AS OF 1 
# MAGIC WHERE reading_1 = 999.99

# COMMAND ----------

# MAGIC %md
# MAGIC ## What just happened?
# MAGIC 
# MAGIC 1. We ingested CSV data from a DBFS location and saved it as a temp view
# MAGIC 1. We loaded our data from the temp views into delta tables
# MAGIC 1. We realized we had some missing "backfill" data so we inserted and updated rows using a merge command
# MAGIC 1. We realized we had some bad data ("999.99") so we updated those rows with a local average then merged the changes back in
# MAGIC 1. Quickly looked at our table history and time travel functionality. 
# MAGIC 
# MAGIC What's next? We have prepared our dataset for a machine learning training example. 
