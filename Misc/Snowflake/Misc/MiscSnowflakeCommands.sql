
-- Create and select table  
CREATE DATABASE IF NOT EXISTS DEMO;
USE DEMO;

-- create/use a schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS rac_schema;
USE schema rac_schema; 

CREATE OR REPLACE FILE FORMAT rac_csv_file
TYPE = CSV
field_delimiter = ','
COMMENT = 'DEMO_CSV_FILE_FORMAT';



-- Create External Stage 
CREATE OR REPLACE STAGE rac_ext_stage_demo
URL = ''
//STORAGE_INTEGRATION = rac_storage_integration  
CREDENTIALS = (AZURE_SAS_TOKEN = '')
FILE_FORMAT = rac_csv_file
;


list @rac_ext_stage_demo;


CREATE OR REPLACE TABLE  FactProductInventory 
AS SELECT t.$1, t.$2, t.$3
FROM @rac_ext_stage_demo 
//FILE_FORMAT = (format_name = 'rac_csv_file');
(file_format => 'rac_csv_file', pattern=>'.*csv') t
;

 --------------------------------------------------------KAFKA STUFF-------------------------------------------------------------------------------
 
 
CREATE DATABASE IF NOT EXISTS DEMO;
USE DEMO;

-- create/use a schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS rac_schema;
USE schema rac_schema; 


CREATE TABLE IF NOT EXISTS kafka_events_bronze (
eventId varchar(50), 
action varchar(10),
time number
  )
;

-- SELECT * FROM kafka_events limit 10

-- Create a private key for the user 
alter user demo_user set rsa_public_key='';
                                        
describe user demo_user


select * from kafka_events_bronze

SELECT * FROM rac_schema.kafka_test_messages

SELECT * FROM RYAN_CHYNOWETH_KAFKA_TEST


use demo;
grant usage on schema rac_schema to role accountadmin;
grant create table on schema rac_schema to role accountadmin;
grant create stage on schema rac_schema to role accountadmin;
grant create pipe on schema rac_schema to role accountadmin;
grant role accountadmin to user demo_user;
alter user demo_user set default_role = accountadmin;





