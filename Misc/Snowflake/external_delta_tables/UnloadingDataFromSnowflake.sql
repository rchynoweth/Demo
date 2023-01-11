CREATE DATABASE IF NOT EXISTS demo;
USE DATABASE demo;

CREATE SCHEMA IF NOT EXISTS rac_schema;
USE SCHEMA rac_schema;


-- Create External Stage 
CREATE OR REPLACE STAGE rac_ext_stage_demo
URL = 'azure://<account>.blob.core.windows.net/<container>'
CREDENTIALS = (AZURE_SAS_TOKEN = '<sas token>')
;


list @rac_ext_stage_demo;


COPY INTO @rac_ext_stage_demo/snowflake_parquet/ 
FROM (
SELECT action, time, to_timestamp(time) as datetime
FROM vw_delta_table_manifest
)
FILE_FORMAT = (TYPE = PARQUET) ;


LIST @rac_ext_stage_demo/snowflake_parquet;



