# Snowflake Examples 

This folder contains various demos related to Snowflake. 

Available Examples: 
1. Delta Tables and Snowflake  
   - Shows two different methods for reading Delta tables with Snowflake 


## Delta Tables and Snowflake 

When working with Delta tables in Snowflake, engineers have two options. The first option is likely the best method at this time as it is in preview status while the second method is experimental. Option one is to use the Snowflake `TABLE_FORMAT=DELTA` which parses the Delta transaction logs to format an external table. The second option is a Databricks process that generates a manifest file that contains the parquet files for the current version of the delta table. Please reference below for more details. 

 - [Using the Snowflake method (transaction log parsing)](https://docs.snowflake.com/en/user-guide/tables-external-intro.html#delta-lake-support)   
    - To create an external table set the `TABLE_FORMAT = DELTA` in the `CREATE EXTERNAL TABLE` statement.  
    - With this support process Snowflake will parse the transaction logs and determine which parquet files to read. In the background the refresh performs add and remove file operations to keep the metadata in sync. 
    - Since DDL operations cannot ensure that event notifications are in the correct order then the table cannot be set to auto refresh

 - [Using the Databricks Manifest File Method](https://docs.databricks.com/delta/snowflake-integration.html)
   - This is the databricks integration for Snowflake. This process will generate a manifest file, which is a text file containing hte list of data files to read for the delta table. This eliminates the need for Snowflake to parse the transaction logs making the process faster.
   - This is experimental. 
   - Note - I built this demo in Azure which originally threw an error when generating the manifest file in Databricks since I was not using S3, running the command below allowed me to generate the manifest file in ADLS.  
   `SET spark.databricks.delta.symlinkFormatManifest.fileSystemCheck.enabled = false`.  


Please note that reading Delta tables in Snowflake is subject to external table limitations (e.g. read only tables). 

