import os 
from snowflake.snowpark import Session 

connection_parameters = {
    "account": "ui14160",
    "user": "demo_user",
    "password": "9lS$zeh6TI61XduQCS3R", 
    "role": "accountadmin",
    "warehouse": "demo_wh",
    "database": "demo",
    "schema": "rac_schema"
}

session = Session.builder.configs(connection_parameters).create()

# test connection
print(session.sql("select current_warehouse(), current_database(), current_schema()").collect())  


df = session.sql("select * from test_stream_table1")
df.show()


df2 = session.table("test_stream_table1")
df2.show()


df.write.mode("overwrite").saveAsTable("test_snowpark_table1")

df = session.sql("select * from test_stream_table1_stream")
df.show()
df.write.mode("overwrite").saveAsTable("test_stream_snowpark")
df = session.sql("select * from test_stream_table1_stream")
df.show()
session.sql("select * from test_stream_snowpark").show()