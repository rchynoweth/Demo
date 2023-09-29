# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake - Non Uniform Example

# COMMAND ----------

from pyspark.sql.functions import *
# from libs.bq_connect import BQConnect
import time 

# COMMAND ----------

dbutils.widgets.text('CatalogName', 'main') # assumes catalog exists
dbutils.widgets.text('SchemaName', '')
dbutils.widgets.text('BQProject', '')

# COMMAND ----------

catalog_name = dbutils.widgets.get('CatalogName')
schema_name = dbutils.widgets.get('SchemaName')
bq_project = dbutils.widgets.get('BQProject')

# COMMAND ----------

spark.sql(f'use catalog {catalog_name}')
spark.sql(f'create schema if not exists {schema_name}')
spark.sql(f'use schema {schema_name}')

# COMMAND ----------

bucket_name = 'racgcs'
storage_location = f'gs://{bucket_name}/ext_products_non_uniform'
dbutils.fs.ls(storage_location)

# COMMAND ----------

# drop if exists 
drop_table = False
if drop_table == True:
  print('dropping table... and deleting data..')
  spark.sql("drop table if exists ext_products_non_uniform")
  dbutils.fs.rm(storage_location, True)

# COMMAND ----------


# create table 
spark.sql(f"""
CREATE TABLE IF NOT EXISTS ext_products_non_uniform (
    product_id STRING,
    product_category STRING,
    product_name STRING,
    sales_price STRING,
    EAN13 STRING,
    EAN5 STRING,
    product_unit STRING
)
LOCATION '{storage_location}/ext_products_non_uniform'
;
""")


# should be empty
display(spark.read.table('ext_products_non_uniform'))

# COMMAND ----------

df = spark.read.format('csv').option('header', 'true').option('delimiter', ';').load('/databricks-datasets/retail-org/products/*.csv')
display(df)

# COMMAND ----------

df.write.mode('append').saveAsTable('ext_products_non_uniform')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from ext_products_non_uniform

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Manifest File 

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.symlinkFormatManifest.fileSystemCheck.enabled = false

# COMMAND ----------

from delta import DeltaTable

# COMMAND ----------

deltaTable = DeltaTable.forPath(spark, path=f'{storage_location}')
deltaTable.generate("symlink_format_manifest")

# COMMAND ----------


