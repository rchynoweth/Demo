-- Create Database and Schema 
CREATE DATABASE IF NOT EXISTS demo;
USE DATABASE demo;

CREATE SCHEMA IF NOT EXISTS rac_schema;
USE SCHEMA rac_schema;



CREATE OR REPLACE TABLE test_stream_table 
(
    id int IDENTITY(1,1),
    string_value string, 
    modified_datetime timestamp default current_timestamp()
); 

INSERT INTO test_stream_table (string_value)
VALUES ('test_one'), ('test_two'), ('test_three'), ('test_four'),('test_five'),('test_six'),('test_seven'),('test_eight'),('test_nine'),('test_ten');


SELECT * FROM test_stream_table;


CREATE OR REPLACE STREAM rac_test_stream
ON TABLE test_stream_table 
APPEND_ONLY = FALSE -- gives updates and deletes
SHOW_INITIAL_ROWS = TRUE ; -- for the initial rows for the first pull then only new/updated rows 



-- Streams are essentially another table so we will select from the stream
SELECT * FROM rac_test_stream;


-- Create a target table 
CREATE OR REPLACE TABLE test_cdc_sink_table 
AS SELECT id, string_value, modified_datetime FROM rac_test_stream;


-- data view
SELECT * FROM test_cdc_sink_table

-- NOTICE - streams are automatically incremented 
SELECT * FROM rac_test_stream;

-- insert,update, and delete 
INSERT INTO test_stream_table (string_value)
VALUES ('test_eleven');


UPDATE test_stream_table SET string_value = 'test_twelve' WHERE id = 1; -- doesn't automatically update the datetime, is that possible with triggers or something? 

DELETE FROM test_stream_table WHERE id = 2;

SELECT * FROM test_stream_table;

-- New stream data 
SELECT * FROM rac_test_stream;


-- complete the merge to the sink 
MERGE INTO test_cdc_sink_table AS target 
USING (SELECT * FROM rac_test_stream) as source 
ON source.id = target.id 
-- updates - we will ignore when action = delete and update = true since they are the same row 
WHEN MATCHED and source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = TRUE THEN 
    UPDATE SET target.string_value = source.string_value, target.modified_datetime = current_timestamp()

-- deletes 
WHEN MATCHED and source.METADATA$ACTION = 'DELETE' and source.METADATA$ISUPDATE = FALSE THEN DELETE 

-- inserts 
WHEN NOT MATCHED and source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = FALSE THEN INSERT (id, string_value, modified_datetime) values (source.id, source.string_value, current_timestamp()) ;




SELECT * FROM test_cdc_sink_table;

-- view stream data again 
SELECT * FROM rac_test_stream;









