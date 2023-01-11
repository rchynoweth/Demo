# Databricks notebook source
# MAGIC %md
# MAGIC # Reading CDC Changes from Snowflake  
# MAGIC 
# MAGIC The Snowflake Spark connector is a great option for reading from and writing data to Snowflake, however, for larger data sizes it can be a bottle. It is recommended that for larger data sizes that users instead initial copy to an external stage from Snowflake as parquet files, then use Databricks Auto Loader to read the staged files.   
# MAGIC 
# MAGIC However, what if I only want to read new data from Snowflake or I want to simulate a stream out of Snowflake. 
# MAGIC 
# MAGIC ## Required Setup in Snowflake  
# MAGIC In order to "stream" data efficiently out of Snowflake we will need to leverage some of Snowflake's CDC capabilities. This requires creating objects in Snowflake that unload data as files to cloud storage. Once in cloud storage we (Databricks) can use Auto Loader to read the files and write them to Delta. Auto Loader can be used as a DLT Pipeline (which I will **not** be showing here) or the Structured Streaming APIs (which I **will** be showing in this notebook). The following will need to be done for each Snowflake table that you want to read. This is due to the nature of Snowflake streams which tracks table changes using offsets.  
# MAGIC ```sql
# MAGIC ---- SET UP FOR ALL TABLES ----
# MAGIC 
# MAGIC -- Create Database
# MAGIC CREATE DATABASE IF NOT EXISTS <NAME OF YOUR SNOWFLAKE DATABASE>;
# MAGIC USE DATABASE <NAME OF YOUR SNOWFLAKE DATABASE>;
# MAGIC -- Create Schema 
# MAGIC CREATE SCHEMA IF NOT EXISTS <NAME OF YOUR SNOWFLAKE SCHEMA>;
# MAGIC USE SCHEMA <NAME OF YOUR SNOWFLAKE SCHEMA>;
# MAGIC 
# MAGIC -- Create a parquet file format to unload data 
# MAGIC CREATE OR REPLACE FILE FORMAT <NAME OF YOUR FILE FORMAT> 
# MAGIC TYPE = PARQUET;
# MAGIC 
# MAGIC -- Create an external stage - this is where I will unload data 
# MAGIC CREATE OR REPLACE STAGE <NAME OF YOUR STAGE>
# MAGIC URL = 'azure://<account>.blob.core.windows.net/<container>'
# MAGIC CREDENTIALS = (AZURE_SAS_TOKEN = '<SAS TOKEN>')
# MAGIC FILE_FORMAT = <NAME OF YOUR FILE FORMAT>
# MAGIC ;
# MAGIC 
# MAGIC 
# MAGIC ---- SET UP FOR EACH TABLE ---- 
# MAGIC 
# MAGIC -- Create a snowflake stream
# MAGIC CREATE OR REPLACE STREAM <NAME OF YOUR STREAM>
# MAGIC ON TABLE <NAME OF YOUR TABLE> 
# MAGIC APPEND_ONLY = FALSE -- gives updates and deletes
# MAGIC SHOW_INITIAL_ROWS = TRUE ; -- for the initial rows for the first pull then only new/updated rows 
# MAGIC 
# MAGIC -- Create a task that runs every minute 
# MAGIC CREATE OR REPLACE TASK <NAME OF YOUR TASK> 
# MAGIC SCHEDULE = '1 MINUTE' -- Change as needed 
# MAGIC ALLOW_OVERLAPPING_EXECUTION = FALSE -- if they overlap then we may get duplicates from the stream if the previous DML is not complete 
# MAGIC USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL' -- using Snowflake Serverless compute 
# MAGIC AS 
# MAGIC (
# MAGIC   COPY INTO @<NAME OF YOUR STAGE>/<STORAGE DIR>/
# MAGIC   FROM (
# MAGIC   SELECT * FROM <NAME OF YOUR STREAM>
# MAGIC       )
# MAGIC   include_query_id=true;  -- Ensures that each file we write has a unique name which is required for auto loader  
# MAGIC )
# MAGIC 
# MAGIC ```
# MAGIC 
# MAGIC <br></br>
# MAGIC Items to ponder: 
# MAGIC - Frequency of data loads:  
# MAGIC   - If you are unloading data out of Snowflake frequently (less than 10 minutes) it is likely best to run your Auto Loader stream 24/7 or use DLT. 
# MAGIC   - Less frequent unloading of data from Snowflake can likely be scheduled using `.trigger(once=True)`. If you do this you will want to try and align this with your Snowflake unload but it will be difficult to perfectly time it. For example, if data is loaded every 30 minutes (1:00, 1:30, ... 3:30,...) then maybe you want to schedule the Databricks job to run five minutes after (1:05, 1:35,...)  
# MAGIC - This is not a streaming solution and should not be advised as "good" architecture. This is help alliviate the pain of trying to load data out of Snowflake in a scalable and repeatable fashion. 
# MAGIC - Not all transactions will be reported. If multiple updates occur on a single row in between executions then only the latest update will be reported. This is due to the nature of Snowflake Streams. 
# MAGIC   - If this is unacceptable then users can use a `changes` clause which will allow users to manually implement CDC operations on Snowflake tables, but please note that the offsets are not managed by Snowflake. 
# MAGIC - This method reduces the total cost of reading data from Snowflake so that it can be used by other tools like Databricks.  
# MAGIC   - Spark connector acquires a double spend (Databricks and Snowflake) and this method is only Snowflake 
# MAGIC   - Other methods may require each user to read the data (i.e. a team of 10 people are reading the same base tables in Snowflake) which means that not only are individuals re-reading data but the entire team is duplicating this effort. Getting data out of Snowflake and into ADLS reduces the number of reads on a table to 1. 
# MAGIC - 

# COMMAND ----------

dbutils.widgets.text("secretScope", "")
dbutils.widgets.text("snowflakeAccount", "") # https://<snowflake_account>.snowflakecomputing.com/ 
dbutils.widgets.text("snowflakeDatabase", "")
dbutils.widgets.text("snowflakeSchema", "")
dbutils.widgets.text("snowflakeWarehouse", "")
dbutils.widgets.text("snowflakeStream", "")
dbutils.widgets.text("databricksSchema", "")

# COMMAND ----------

secret_scope = dbutils.widgets.get("secretScope")
snowflake_account = dbutils.widgets.get("snowflakeAccount")
snowflake_database = dbutils.widgets.get("snowflakeDatabase")
snowflake_schema = dbutils.widgets.get("snowflakeSchema")
snowflake_warehouse = dbutils.widgets.get("snowflakeWarehouse")
snowflake_stream = dbutils.widgets.get("snowflakeStream")
databricks_schema = dbutils.widgets.get("databricksSchema")

# COMMAND ----------

spark.sql(f"USE {databricks_schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set up Snowflake 

# COMMAND ----------



# COMMAND ----------

# Use dbutils secrets to get Snowflake credentials.
user = dbutils.secrets.get(secret_scope, "snowflake_user")
password = dbutils.secrets.get(secret_scope, "snowflake_password")
 
options = {
  "sfUrl": f"{snowflake_account}.snowflakecomputing.com",
  "sfUser": user,
  "sfPassword": password,
  "sfDatabase": snowflake_database,
  "sfSchema": snowflake_schema,
  "sfWarehouse": snowflake_warehouse
}

# COMMAND ----------

df = (spark.read
  .format("snowflake")
  .options(**options)
  .option("dbtable", "rac_test_stream")
  .load()
 )

display(df)

# COMMAND ----------

display(df) # display does nothing

# COMMAND ----------

df.write.saveAsTable(snowflake_stream)

# COMMAND ----------

display(df)

# COMMAND ----------

df = (spark.read
  .format("snowflake")
  .options(**options)
  .option("dbtable", "rac_test_stream")
  .load()
 )

display(df)

# COMMAND ----------


