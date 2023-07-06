# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake 3.0 - Uniform Example
# MAGIC
# MAGIC Delta Lake 3.0 introduces the ability to write data as not only a Delta Lake table, but persists the metadata required for Hudi and Iceberg as well. 
# MAGIC
# MAGIC Demo Requirements:
# MAGIC 1. DBR 13.2 or greater 
# MAGIC 1. A Google Service Account attached to the cluster in order to list GCS objects   
# MAGIC     - See "Advanced options"
# MAGIC 1. GCP Databricks 
# MAGIC
# MAGIC This notebook will provide a brief example on how to leverage this capability. We will do it in two different ways:
# MAGIC 1. Create a brand new table with the appropriate settings then insert data into the table 
# MAGIC 1. Write data as a Delta Lake table and "upgrade" the table to Uniform
# MAGIC 1. Optional - Repeat the first step to create an external table that we will access directly with BQ. 
# MAGIC
# MAGIC
# MAGIC Resources: 
# MAGIC - [Protocol.md](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-icebergcompatv1)  
# MAGIC - [Column Mapping](https://docs.databricks.com/delta/delta-column-mapping.html)  
# MAGIC - [Uniform](https://docs.databricks.com/delta/uniform.html)

# COMMAND ----------

from pyspark.sql.functions import *
import time 

# COMMAND ----------

dbutils.widgets.text('CatalogName', 'main') # assumes catalog exists
dbutils.widgets.text('SchemaName', '')

# COMMAND ----------

catalog_name = dbutils.widgets.get('CatalogName')
schema_name = dbutils.widgets.get('SchemaName')

# COMMAND ----------

spark.sql(f'use catalog {catalog_name}')
spark.sql(f'create schema if not exists {schema_name}')
spark.sql(f'use schema {schema_name}')

# COMMAND ----------

spark.sql("SET spark.databricks.delta.properties.defaults.minReaderVersion = 2;")
spark.sql("SET spark.databricks.delta.properties.defaults.minWriterVersion = 7;")
spark.sql("SET spark.databricks.delta.write.dataFilesToSubdir = true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Empty Table 

# COMMAND ----------

# drop if exists 
spark.sql("drop table if exists products")

# create table 
spark.sql("""
CREATE TABLE products (
    product_id STRING,
    product_category STRING,
    product_name STRING,
    sales_price STRING,
    EAN13 STRING,
    EAN5 STRING,
    product_unit STRING
)
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')
;
""")


# should be empty
display(spark.read.table('products'))

# COMMAND ----------

table_location = spark.sql('describe extended products').filter(col('col_name')=='Location').select('data_type').collect()[0][0]
dbutils.fs.ls(table_location)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC NOTICE!! - Do you see the `metadata` folder in the table directory? That is the iceberg metadata for readers to leverage. Let's take a closer look. 

# COMMAND ----------

dbutils.fs.ls(table_location+'/metadata')

# COMMAND ----------

dbutils.fs.ls(table_location+'/_delta_log')

# COMMAND ----------

df = spark.read.format('csv').option('header', 'true').option('delimiter', ';').load('/databricks-datasets/retail-org/products/*.csv')
display(df)

# COMMAND ----------

df.write.mode('append').saveAsTable('products')

# COMMAND ----------

dbutils.fs.ls(table_location+'/metadata')

# COMMAND ----------

dbutils.fs.ls(table_location+'/_delta_log')

# COMMAND ----------

# MAGIC %md
# MAGIC NOTICE!! - There are new metadata files for both delta and Iceberg! That is awesome! 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upgrade an Existing Table 

# COMMAND ----------

df = spark.read.format('csv').option('header', 'true').option('delimiter', ';').load('/databricks-datasets/retail-org/products/*.csv')
display(df)

# COMMAND ----------

spark.sql('drop table if exists products2')
df.write.saveAsTable('products2')

# COMMAND ----------

table_location = spark.sql('describe extended products2').filter(col('col_name')=='Location').select('data_type').collect()[0][0]
dbutils.fs.ls(table_location)

# COMMAND ----------

# MAGIC %md
# MAGIC NOTICE!! - only the `_delta_log` directory exists on this table 

# COMMAND ----------

spark.sql("""  
ALTER TABLE products2 SET TBLPROPERTIES (
    -- 'delta.minReaderVersion' = '2', -- only if you don't have the spark settings
    -- 'delta.minWriterVersion' = '7', -- only if you don't have the spark settings
    'delta.enableIcebergCompatV1' = 'true',
    'delta.columnMapping.mode' = 'name',
    'delta.universalFormat.enabledFormats' = 'iceberg'
  )
""")

# COMMAND ----------

df.write.mode('append').saveAsTable('products2')

# COMMAND ----------

time.sleep(2)

# COMMAND ----------

dbutils.fs.ls(table_location)

# COMMAND ----------

dbutils.fs.ls(table_location+'/metadata')

# COMMAND ----------

dbutils.fs.ls(table_location+'/_delta_log')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write External Table - Not required 
# MAGIC
# MAGIC In my demo environment, I do not have access to Google Cloud Storage bucket that is associated with the workspace's UC Metastore. As a workaround I will save a table to an external location. Please note that this is **NOT** a requirement. Typically admins can configure direct access to the Metastore Bucket so an external table is not required. 
# MAGIC
# MAGIC Please note that you must have an external location for the following code to work. To create an external location please see the documentation [here](https://docs.gcp.databricks.com/sql/language-manual/sql-ref-syntax-ddl-create-location.html). 

# COMMAND ----------

bucket_name = 'racgcs'
storage_location = f'gs://{bucket_name}'
dbutils.fs.ls(storage_location)

# COMMAND ----------

# drop if exists 
spark.sql("drop table if exists ext_products")
# dbutils.fs.rm(storage_location, True)

# COMMAND ----------


# create table 
spark.sql(f"""
CREATE TABLE ext_products (
    product_id STRING,
    product_category STRING,
    product_name STRING,
    sales_price STRING,
    EAN13 STRING,
    EAN5 STRING,
    product_unit STRING
)
LOCATION '{storage_location}/ext_products'
TBLPROPERTIES ('delta.universalFormat.enabledFormats' = 'iceberg')
;
""")


# should be empty
display(spark.read.table('ext_products'))

# COMMAND ----------

df = spark.read.format('csv').option('header', 'true').option('delimiter', ';').load('/databricks-datasets/retail-org/products/*.csv')
display(df)

# COMMAND ----------

table_location = spark.sql('describe extended ext_products').filter(col('col_name')=='Location').select('data_type').collect()[0][0]
dbutils.fs.ls(table_location)

# COMMAND ----------

df.write.mode('append').saveAsTable('ext_products')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from ext_products

# COMMAND ----------

# MAGIC %md
# MAGIC One of the methods of data access in BigQuery for Iceberg is using the metadata file directly. To do so we need to update the BQ table definition then refresh the table inside of BQ. 
# MAGIC
# MAGIC We will do so with a create or replace table statement

# COMMAND ----------

metadata_path = [f.path for f in dbutils.fs.ls(table_location+'/metadata') if '.json' in f.path][-1]

big_query_tbl_def = f"""
CREATE OR REPLACE EXTERNAL TABLE rac_dataset.ext_products2
OPTIONS (
    format = 'ICEBERG',
    uris = ["{metadata_path}"]
)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion 
# MAGIC
# MAGIC **What have we just done and what do we do next?**
# MAGIC
# MAGIC We created three separate tables. Two of the tables are managed by UC and can be accessed from Iceberg Readers using the __ method. The third table is stored in an external 
# MAGIC
# MAGIC Please note that the two managed tables can also be access directly if admins provide access to the GCS bucket, however, I do not have that permission in my demo environment which is why I created the external table. 
