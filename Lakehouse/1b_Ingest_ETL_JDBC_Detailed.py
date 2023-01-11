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


# COMMAND ----------


# Create the JDBC URL without passing in the user and password parameters.
jdbcUrl = "jdbc:sqlserver://{}:{};database={};user={};password={}".format(
  jdbcHostname, jdbcPort, jdbcDatabase, jdbcUsername, jdbcPassword
)

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

df.createOrReplaceTempView("tmp")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct device_operational_status
# MAGIC FROM tmp

# COMMAND ----------

# Create a temporary view on the dataframes to enable SQL

df.createOrReplaceTempView("historical_bronze_vw")
df_backfill.createOrReplaceTempView("historical_bronze_backfill_vw")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Bronze Historical Tables
# MAGIC 
# MAGIC #### Databricks' Delta Lake is the world's most advanced data lake technology.  
# MAGIC 
# MAGIC Delta Lake brings __*Performance*__ and __*Reliability*__ to Data Lakes
# MAGIC 
# MAGIC Why did Delta Lake have to be invented?  Let's take a look...
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/projects_failing.png?raw=true" width=1000/>
# MAGIC 
# MAGIC As the graphic above shows, Big Data Lake projects have a very high failure rate.  In fact, Gartner Group estimates that 85% of these projects fail (see https://www.infoworld.com/article/3393467/4-reasons-big-data-projects-failand-4-ways-to-succeed.html ).  *Why* is the failure rate so high?
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/projects_failing_reasons.png?raw=true" width=1000/>
# MAGIC 
# MAGIC The graphic above shows the main __*reliability*__ issues with data lakes.  Unlike relational databases, typical data lakes are not capable of transactional (ACID) behavior.  This leads to a number of reliability issues:
# MAGIC 
# MAGIC - When a job fails, incomplete work is not rolled back, as it would be in a relational database.  Data may be left in an inconsistent state.  This issue is extremely difficult to deal with in production.
# MAGIC 
# MAGIC - Data lakes typically cannot enforce schema.  This is often touted as a "feature" called "schema-on-read," because it allows flexibility at data ingest time.  However, when downstream jobs fail trying to read corrupt data, we have a very difficult recovery problem.  It is often difficult just to find the source application that caused the problem... which makes fixing the problem even harder!
# MAGIC 
# MAGIC - Relational databases allow multiple concurrent users, and ensure that each user gets a consistent view of data.  Half-completed transactions never show up in the result sets of other concurrent users.  This is not true in a typical data lake.  Therefore, it is almost impossible to have a concurrent mix of read jobs and write jobs.  This becomes an even bigger problem with streaming data, because streams typically don't pause to let other jobs run!
# MAGIC 
# MAGIC Next, let's look at the key __*performance issues*__ with data lakes...
# MAGIC 
# MAGIC <img src="https://github.com/billkellett/flight-school-resources/blob/master/images/projects_failing_reasons_1.png?raw=true" width=1000/>
# MAGIC 
# MAGIC - We have already noted that data lakes cannot provide a consistent view of data to concurrent users.  This is a reliability problem, but it is also a __*performance*__ problem because if we must run jobs one at a time, our production time window becomes extremely limited.
# MAGIC 
# MAGIC - Most data lake engineers have come face-to-face with the "small-file problem."  Data is typically ingested into a data lake in batches.  Each batch typically becomes a separate physical file in a directory that defines a table in the lake.  Over time, the number of physical files can grow to be very large.  When this happens, performance suffers because opening and closing these files is a time-consuming operation.  
# MAGIC 
# MAGIC - Experienced relational database architects may be surprised to learn that Big Data usually cannot be indexed in the same way as relational databases.  The indexes become too large to be manageable and performant.  Instead, we "partition" data by putting it into sub-directories.  Each partition can represent a column (or a composite set of columns) in the table.  This lets us avoid scanning the entire data set... *if* our queries are based on the partition column.  However, in the real world, analysts are running a wide range of queries which may or may not be based on the partition column.  In these scenarios, there is no benefit to partitioning.  In addition, partitioning breaks down if we choose a partition column with extremely high cardinality.
# MAGIC 
# MAGIC - Data lakes typically live in cloud storage (e.g., S3 on AWS, ADLS on Azure), and these storage devices are quite slow compared to SSD disk drives.  Most data lakes have no capability to cache data on faster devices, and this fact has a major impact on performance.
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
# MAGIC -- Let's try to understand our backfill data
# MAGIC 
# MAGIC DESCRIBE TABLE historical_bronze_backfill_vw

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
# MAGIC MegaCorp has informed us that the Bronze historical data has a few issues.  Let's deal with them and create a clean Silver table.

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

# MAGIC %sql
# MAGIC -- MegaCorp just informed us of some dirty data.  Occasionally they would receive garbled data.
# MAGIC -- In those cases, they would put 999.99 in the readings.
# MAGIC -- Let's find these records
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver
# MAGIC WHERE reading_1 = 999.99

# COMMAND ----------

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

# MAGIC %sql
# MAGIC -- List all the versions of the table that are available to us
# MAGIC 
# MAGIC DESCRIBE HISTORY sensor_readings_historical_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ah, version 1 should have the 999.99 values
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver 
# MAGIC VERSION AS OF 1 
# MAGIC WHERE reading_1 = 999.99

# COMMAND ----------

# MAGIC %md
# MAGIC How could we __*partition*__ the Silver data for faster access?  Suggest a method, and be ready to discuss its pros and cons.  Feel free to imagine the query patterns MegaCorp will be using.
# MAGIC 
# MAGIC To get started, let's examine how partitioning works.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE EXTENDED will give us some partition information, and will also tell us the location of the data
# MAGIC -- Hmmm, looks like we are not partitioned.  What does that mean?
# MAGIC 
# MAGIC DESCRIBE EXTENDED sensor_readings_historical_silver

# COMMAND ----------

# Let's look at the physical file layout in a non-partitioned table

dbutils.fs.ls(f"dbfs:/user/hive/warehouse/{database_name}.db/sensor_readings_historical_silver")

# As you can see, the data is just broken into a set of files, without regard to the meaning of the data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Maybe Date would be a good way to partition the data
# MAGIC 
# MAGIC SELECT DISTINCT DATE(reading_time) FROM sensor_readings_historical_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's create a Silver table partitioned by Device. 
# MAGIC -- Create a new table, so we can compare new and old
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_silver_by_device;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_silver_by_device 
# MAGIC using Delta
# MAGIC partitioned by (device_id) 
# MAGIC as select * from sensor_readings_historical_silver 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- We can see partition information
# MAGIC 
# MAGIC DESCRIBE EXTENDED sensor_readings_historical_silver_by_device

# COMMAND ----------

# Now we have subdirectories for each device, with physical files inside them
# Will that speed up queries?

dbutils.fs.ls(f"dbfs:/user/hive/warehouse/{database_name}.db/sensor_readings_historical_silver_by_device")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's create a Silver table partitioned by BOTH Date AND Hour. 
# MAGIC -- Note that Delta cannot partition by expressions, so I have to explicitly create the partition columns
# MAGIC -- HINT: Use the DATE() function to extract date from a timestamp, and use the HOUR() function to extract hour from a timestamp
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_silver_by_hour;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_silver_by_hour 
# MAGIC USING DELTA
# MAGIC partitioned by (time_date, time_hour)
# MAGIC as select *, DATE(reading_time) as time_date, HOUR(reading_time) as time_hour from sensor_readings_historical_silver 

# COMMAND ----------

# NOTE how the hour directories are nested within the date directories

dbutils.fs.ls(f"dbfs:/user/hive/warehouse/{database_name}.db/sensor_readings_historical_silver_by_hour/time_date=2015-02-24/")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's create a Silver table partitioned by Date AND Hour AND Minute. 
# MAGIC -- Note that Delta cannot partition by expressions, so I have to explicitly create the partition columns
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_silver_by_hour_and_minute;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_silver_by_hour_and_minute 
# MAGIC USING DELTA
# MAGIC partitioned by (time_date, time_hour, time_minute) 
# MAGIC as select *, DATE(reading_time) as time_date, HOUR(reading_time) as time_hour, minute(reading_time) as time_minute from sensor_readings_historical_silver 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Let's take a peek at our minute-partitioned table
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver_by_hour_and_minute
# MAGIC LIMIT 100

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now let's take some timings that compare our partitioned Silver tables against the unpartitioned Silver table
# MAGIC -- Here is an example "baseline" query against the unpartitioned Silver table
# MAGIC -- (run these queries several times to get a rough average)
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver
# MAGIC WHERE 
# MAGIC   DATE(reading_time) = '2015-02-24' AND
# MAGIC   HOUR(reading_time) = '14' AND
# MAGIC   MINUTE(reading_time) = '2'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now compare the time for the same query against a partitioned table
# MAGIC -- Think and discuss... Did both data skipping and partitioning play a part here?  How could you combine data skipping and partitioning to make queries even more performant?
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM sensor_readings_historical_silver_by_hour_and_minute
# MAGIC WHERE 
# MAGIC   time_date = '2015-02-24' AND
# MAGIC   time_hour = '14' AND
# MAGIC   time_minute = '2'

# COMMAND ----------

# MAGIC %md
# MAGIC Imagine one or more Gold tables that Analysts and Data Scientists might want.  Create a few examples, and be ready to discuss your choices.

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_gold_one;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_gold_one
# MAGIC USING DELTA
# MAGIC as select device_type, time_hour, avg(reading_1) as average_reading1, avg(reading_2) as average_reading2, avg(reading_3) as average_reading_3 from sensor_readings_historical_silver_by_hour_and_minute group by device_type, time_hour

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sensor_readings_historical_gold_one

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS sensor_readings_historical_gold_two;
# MAGIC 
# MAGIC CREATE TABLE sensor_readings_historical_gold_two 
# MAGIC USING DELTA 
# MAGIC 
# MAGIC select date(reading_time) as reading_date
# MAGIC   , hour(reading_time) as reading_hour
# MAGIC   , count(*) as device_count
# MAGIC   , device_type, device_operational_status
# MAGIC   , avg(reading_1) as avg1
# MAGIC   , avg(reading_2) as avg2
# MAGIC   , avg(reading_3) as avg3
# MAGIC 
# MAGIC from sensor_readings_historical_silver
# MAGIC 
# MAGIC group by device_type, device_operational_status, date(reading_time), hour(reading_time)
# MAGIC 
# MAGIC order by 1,2

# COMMAND ----------

# MAGIC %sql 
# MAGIC select *
# MAGIC from sensor_readings_historical_gold_two

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema evolution!

# COMMAND ----------

spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")  # set our spark configuration for schema merge

# COMMAND ----------

# MAGIC %sql
# MAGIC -- insert new data with an extra column into the gold table
# MAGIC insert into sensor_readings_historical_gold_two
# MAGIC select *, 1 as new_col
# MAGIC from sensor_readings_historical_gold_two
# MAGIC limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sensor_readings_historical_gold_two -- look at the new column

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sensor_readings_historical_gold_two where new_col is not null -- look at the new column with values

# COMMAND ----------


