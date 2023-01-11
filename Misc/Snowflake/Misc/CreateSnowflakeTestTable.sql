-- Create Database and Schema 
CREATE DATABASE IF NOT EXISTS demo;
USE DATABASE demo;

CREATE SCHEMA IF NOT EXISTS rac_schema;
USE SCHEMA rac_schema;

----------------- CREATE TABLE AND INSERT VALUES 

CREATE OR REPLACE TABLE test_stream_table 
(
    id int IDENTITY(1,1),
    string_value string, 
    modified_datetime timestamp default current_timestamp()
); 

INSERT INTO test_stream_table (string_value)
VALUES ('test_one'), ('test_two'), ('test_three'), ('test_four'),('test_five'),('test_six'),('test_seven'),('test_eight'),('test_nine'),('test_ten');


SELECT * FROM test_stream_table;

----------------- OPERATION TWO 

-- insert,update, and delete 
INSERT INTO test_stream_table (string_value)
VALUES ('test_eleven');


UPDATE test_stream_table SET string_value = 'test_twelve' WHERE id = 1; -- doesn't automatically update the datetime, is that possible with triggers or something? 

DELETE FROM test_stream_table WHERE id = 2;



----------------- OPERATION THREE
-- what happens if I update the same row many times?
-------- Stream will give you the latest row and the oldest row but no intermediate values 
UPDATE test_stream_table SET string_value = 'test_13', modified_datetime=current_timestamp() WHERE id = 1; 
UPDATE test_stream_table SET string_value = 'test_14', modified_datetime=current_timestamp() WHERE id = 1; 
UPDATE test_stream_table SET string_value = 'test_16', modified_datetime=current_timestamp() WHERE id = 1; 




------- other 
  show tasks like '%test_stream_table%';

  alter task if exists test_stream_table resume;
