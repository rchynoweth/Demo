----------------------------------------
----- Snowflake - Transaction Log Parsing 
-----   Run the below commands to create an external table using the manifest method. 
-----   Link: https://docs.snowflake.com/en/user-guide/tables-external-intro.html#delta-lake-support
----------------------------------------

-- Create Database and Schema 
CREATE DATABASE IF NOT EXISTS demo;
USE DATABASE demo;

CREATE SCHEMA IF NOT EXISTS rac_schema;
USE SCHEMA rac_schema;


-- Create External Stage 
CREATE OR REPLACE STAGE rac_ext_stage_demo
URL = 'azure://<account>.blob.core.windows.net/<container>'
CREDENTIALS = (AZURE_SAS_TOKEN = '<SAS TOKEN>')
;


list @rac_ext_stage_demo;

-- Create External Table
CREATE EXTERNAL TABLE delta_table
  LOCATION = @rac_ext_stage_demo/delta_table
  AUTO_REFRESH = FALSE
  FILE_FORMAT = (type = PARQUET) 
  TABLE_FORMAT = DELTA;
  

-- View Data
SELECT * FROM delta_table;

-- Refresh External Table
ALTER EXTERNAL TABLE delta_table REFRESH; 

SELECT * FROM delta_table;


----------------------------------------
----- Databricks Reader (Experimental)  
-----   Run the below commands to create an external table using the manifest method. 
-----   Link: https://docs.databricks.com/delta/snowflake-integration.html
----------------------------------------


-- Create Database and Schema 
CREATE DATABASE IF NOT EXISTS demo;
USE DATABASE demo;

CREATE SCHEMA IF NOT EXISTS rac_schema;
USE SCHEMA rac_schema;


-- Create External Stage 
CREATE OR REPLACE STAGE rac_ext_stage_demo
URL = 'azure://<account>.blob.core.windows.net/<container>'
CREDENTIALS = (AZURE_SAS_TOKEN = '<sas token>')
;

-- view stage
list @rac_ext_stage_demo;


-- Create External Table for Manifest file 
CREATE OR REPLACE EXTERNAL TABLE delta_table_manifest_file (
    filename VARCHAR AS split_part(VALUE:c1, '/', -1)
)
LOCATION = @rac_ext_stage_demo/delta_table_manifest/_symlink_format_manifest/
FILE_FORMAT = (TYPE = CSV)
PATTERN = '.*[/]manifest'
AUTO_REFRESH = FALSE;

SELECT * FROM delta_table_manifest_file;


-- Create External Table for Parquet Files 
CREATE OR REPLACE EXTERNAL TABLE delta_table_manifest
(
    action VARCHAR AS (VALUE:action::VARCHAR),
    time BIGINT AS (VALUE:time::BIGINT),
    parquet_filename VARCHAR AS (split_part(metadata$filename, '/', -1))
)
LOCATION = @rac_ext_stage_demo/delta_table_manifest
FILE_FORMAT = (TYPE = PARQUET)
PATTERN = '.*[/]part-[^/]*[.]parquet'
AUTO_REFRESH = FALSE;


SELECT * FROM delta_table_manifest;


-- Create view to only read the parquet files that are in the manifest table 
CREATE OR REPLACE VIEW vw_delta_table_manifest AS
    SELECT action, time
    FROM delta_table_manifest
    WHERE parquet_filename IN (SELECT filename FROM delta_table_manifest_file);
    
 SELECT * FROM vw_delta_table_manifest;


-- At least in Azure the tables cannot be refreshed automatically 
-- Note that the manifest file must be updated from Databricks, then refreshed in Snowflake 
ALTER EXTERNAL TABLE delta_table_manifest REFRESH;
ALTER EXTERNAL TABLE delta_table_manifest_file REFRESH;


