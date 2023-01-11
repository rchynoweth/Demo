# Databricks notebook source
# MAGIC %md
# MAGIC # Ingesting Data from a RDBMS and ETL 
# MAGIC 
# MAGIC Databricks has built in support for [JDBC](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html) connections to all databases that support it. In addition, there are often high performance connectors for specific Databases like [Azure Synapse DW]() or [Azure SQL Database](). 
# MAGIC 
# MAGIC Because of this connectivity, Databricks can be used to [connect directly to source systems](https://databricks.com/blog/2019/03/08/securely-accessing-external-data-sources-from-databricks-for-aws.html) and ingest the data for analytics. Whether these systems are on-premises or in the cloud, corporate policies usually have firewalls enabled to prevent unwanted access. This will require a VNET or VPC injected deployment of a Databricks workspace so that the networking is appropriate. In this scenario I have a demo database that does not have this restriction, but please note the code is the same regardless.  
# MAGIC 
# MAGIC Below is a sample architecture where users leverage Databricks to ingest data from source systems.   
# MAGIC <br></br>
# MAGIC <img src="https://racadlsgen2.blob.core.windows.net/public/LakehouseArchitecture.png" width=900/>
# MAGIC <br></br>
# MAGIC Please note that as an alternative some customers use third party tools that push data to the cloud instead of pulling data from systems. In these scenarios we often leverage the [Databricks AutoLoader](https://docs.databricks.com/spark/latest/structured-streaming/auto-loader.html) or the [COPY INTO](https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-copy-into.html) functionality of Spark SQL.  

# COMMAND ----------

dbutils.widgets.text("DatabaseName", "rac_demo_db")
dbutils.widgets.text("UserName", "ryan.chynoweth@databricks.com")
database_name = dbutils.widgets.get("DatabaseName")
user_name = dbutils.widgets.get("UserName")
spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
spark.sql("USE {}".format(database_name))

# COMMAND ----------

jdbcUsername = dbutils.secrets.get(scope = "rac_scope", key = "azuresqluser")
jdbcPassword = dbutils.secrets.get(scope = "rac_scope", key = "azuresqlpassword")
jdbcHostname = dbutils.secrets.get(scope = "rac_scope", key = "azuresqlserver")
jdbcPort = dbutils.secrets.get(scope = "rac_scope", key = "azuresqlport")
jdbcDatabase = dbutils.secrets.get(scope = "rac_scope", key = "azuresqldatabase")
jdbcUrl = "jdbc:sqlserver://{}:1433;database={};user={}@{};password={};".format(jdbcHostname, jdbcDatabase, jdbcUsername, jdbcDatabase, jdbcPassword)

# COMMAND ----------

dbutils.notebook.run("./includes/1b_setup", 0, {"database_name": database_name, "user_name": user_name})

# COMMAND ----------

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

df = (spark.read
 .format("jdbc")
 .option("url", jdbcUrl)
 .option("dbtable", "rac_lakehouse_ingest")
 .load())

display(df)

# COMMAND ----------

df_backfill = (spark.read
 .format("jdbc")
 .option("url", jdbcUrl)
 .option("dbtable", "rac_lakehouse_ingest_backfill")
 .load())

display(df_backfill)

# COMMAND ----------

# Create a temporary view on the dataframes to enable SQL
df.createOrReplaceTempView("historical_bronze_vw")
df_backfill.createOrReplaceTempView("historical_bronze_backfill_vw")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Bronze Historical Tables
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

# DBTITLE 1,Create a Bronze Table
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

# MAGIC %md
# MAGIC ### Create Silver table
# MAGIC 
# MAGIC We have a few data issues upon ingestion that we will need to fix in as we move data into our silver layer. 
# MAGIC 
# MAGIC - Missing data that we want to merge in because there are some existing records that are incorrect too
# MAGIC - `999.99` data issue that we will want to replace with a local average

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

# DBTITLE 1,Merge backfill data so that we fix data issues and add back missing data
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

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver
# MAGIC WHERE reading_1 = 999.99

# COMMAND ----------

# DBTITLE 1,Compute local averages using some of our Spark SQL functionality 
# MAGIC %sql
# MAGIC -- We want to fix these bogus readings.  Here's the idea...
# MAGIC -- - Use a SQL window function to order the readings by time within each device
# MAGIC -- - Whenever there is a 999.99 reading, replace it with the AVERAGE of the PREVIOUS and FOLLOWING readings.
# MAGIC -- HINTS:
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

# DBTITLE 1,Merge the averages into our silver table
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

# MAGIC %sql
# MAGIC -- Now make sure we got rid of all the bogus readings.
# MAGIC -- Gee, this is fast.  Why?  What feature in Delta Lake is making this so speedy?
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver
# MAGIC WHERE reading_1 = 999.99

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


