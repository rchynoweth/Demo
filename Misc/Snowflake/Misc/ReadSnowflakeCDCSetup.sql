-- Create Database and Schema 
CREATE DATABASE IF NOT EXISTS demo;
USE DATABASE demo;

CREATE SCHEMA IF NOT EXISTS rac_schema;
USE SCHEMA rac_schema;


-- Create a parquet file format to unload data 
CREATE OR REPLACE FILE FORMAT rac_parquet 
TYPE = PARQUET;

-- Create an external stage 
-- this is where I will unload data 
CREATE OR REPLACE STAGE rac_ext_stage_demo_parquet
URL = 'azure://<account>.blob.core.windows.net/<container>'
CREDENTIALS = (AZURE_SAS_TOKEN = '<SAS TOKEN>')
FILE_FORMAT = rac_parquet
;

-- Create a source table and insert data 
CREATE OR REPLACE TABLE test_stream_table 
(
    id int IDENTITY(1,1),
    string_value string, 
    modified_datetime timestamp default current_timestamp()
); 

INSERT INTO test_stream_table (string_value)
VALUES ('test_one'), ('test_two'), ('test_three'), ('test_four'),('test_five'),('test_six'),('test_seven'),('test_eight'),('test_nine'),('test_ten');


SELECT * FROM test_stream_table;


-- Create a snowflake stream
CREATE OR REPLACE STREAM rac_test_stream
ON TABLE test_stream_table 
APPEND_ONLY = FALSE -- gives updates and deletes
SHOW_INITIAL_ROWS = TRUE ; -- for the initial rows for the first pull then only new/updated rows 


select * from rac_test_stream;

-- unload data to the stage 
copy into @rac_ext_stage_demo_parquet/cdc_unload/
from (
select * from rac_test_stream
    )
include_query_id=true ; -- ensures unique file names 
    
-- NOTICE - streams are automatically incremented 
SELECT * FROM rac_test_stream;

-- Make changes to the source table - insert,update, and delete 
INSERT INTO test_stream_table (string_value)
VALUES ('test_eleven');


UPDATE test_stream_table SET string_value = 'test_twelve' WHERE id = 1; -- doesn't automatically update the datetime, is that possible with triggers or something? 

DELETE FROM test_stream_table WHERE id = 2;


SELECT * FROM rac_test_stream;

-- Unload data to the stage 
copy into @rac_ext_stage_demo_parquet/cdc_unload/
from (
select * from rac_test_stream
    )
include_query_id=true; -- ensures unique files names 


SELECT * FROM rac_test_stream;

-- what happens if I update the same row many times?
-- gives you the latest row and the oldest row but no intermediate values 
UPDATE test_stream_table SET string_value = 'test_13', modified_datetime=current_timestamp() WHERE id = 1; 
UPDATE test_stream_table SET string_value = 'test_14', modified_datetime=current_timestamp() WHERE id = 1; 
UPDATE test_stream_table SET string_value = 'test_15', modified_datetime=current_timestamp() WHERE id = 1; 

-- Unload data to the stage 
copy into @rac_ext_stage_demo_parquet/cdc_unload/
from (
select * from rac_test_stream
    )
include_query_id=true;    



CREATE OR REPLACE TASK rac_test_data_cdc_unload 
SCHEDULE = '1 MINUTE'
ALLOW_OVERLAPPING_EXECUTION = FALSE 
-- WAREHOUSE = 'DEMO_WH'
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
AS 
    copy into @rac_ext_stage_demo_parquet/cdc_unload/
    from (
    select * from rac_test_stream
        )
    include_query_id=true;    


DESCRIBE TASK rac_test_data_cdc_unload ;

select *
from table(information_schema.task_history())
where name = 'RAC_TEST_DATA_CDC_UNLOAD'
order by query_start_time desc;


alter task if exists rac_test_data_cdc_unload resume;


UPDATE test_stream_table SET string_value = 'test_22', modified_datetime=current_timestamp() WHERE id = 1; 


alter task if exists rac_test_data_cdc_unload suspend;
