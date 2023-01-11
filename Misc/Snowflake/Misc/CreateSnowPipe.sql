-- Create Database and Schema 
CREATE DATABASE IF NOT EXISTS demo;
USE DATABASE demo;

CREATE SCHEMA IF NOT EXISTS rac_schema;
USE SCHEMA rac_schema;






CREATE OR REPLACE STAGE rac_ext_stage_demo
URL = 'azure://<account>.blob.core.windows.net/<container>'
CREDENTIALS = (AZURE_SAS_TOKEN = '<SAS TOKEN>')
;


-- view stage
list @rac_ext_stage_demo/event_json_data;

-- create a target table 
create or replace table rac_event_json (payload variant);
select * from rac_event_json;


-- test query 
select t.* from @rac_ext_stage_demo/event_json_data/file-0.json (file_format => rac_json_format) t;


-- copy into staging table 
copy into rac_event_json 
from ( select t.$1 from @rac_ext_stage_demo/event_json_data/file-0.json (file_format => rac_json_format) t ) ;

-- select data from staging table 
select * from rac_event_json;
select payload:action::string as action, payload:time as time from rac_event_json ;

-- create a pipe to copy data into stage 
create pipe if not exists rac_json_pipe as
copy into rac_event_json from ( select t.$1 from @rac_ext_stage_demo/event_json_data/file-0.json (file_format => rac_json_format) t ) ;


alter pipe rac_json_pipe refresh;


select system$pipe_status('rac_json_pipe');